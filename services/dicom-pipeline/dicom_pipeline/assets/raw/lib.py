from datetime import datetime
import inflection as inf
import numpy as np
import pandas as pd
import pydicom
import os
import json
from dagster import (
	AssetExecutionContext, 
	AutoMaterializePolicy,
	AssetIn,
	MetadataValue,
    asset
)
from .config import RawConfig
from ...resources.config import (
	COLUMN_PARTITION_MAP,
	DBT_PROJECT_NAME,
	PARTITION_MONTHLY
)

def read_dicom_file_to_df(
	context: AssetExecutionContext,
	config: RawConfig
) -> pd.DataFrame:
    """
    Common function that collects certain list of columns from
    local source DICOM file, normalizes names to snake case and does
    type conversion from raw data based on description obtained from
	pydicom. List of columns to collect depends on the underlying entity;
	One of the: <devices, images, patients, series, studies> - columns are
	defined in corresponding modules. Sequences are stored as pandas DataFrame.
    """
    meta = []
    for filepath in config.files:
        context.log.info(filepath)
        with pydicom.dcmread(filepath) as obj:
            if config.partition_column is not None:
                camel_partition_column = inf.camelize(config.partition_column)
                target_date = pd.to_datetime(context.asset_partition_key_for_output())
                file_date = pd.to_datetime(obj[camel_partition_column].value, format="%Y%m%d")
                if file_date.month != target_date.month or file_date.year != target_date.year:
                    continue
			# Track specific dicom files we read from
            tmp = []
            for key in config.columns:
                tmp.append(cast(context, obj.get(key, np.nan)))
            tmp.append(filepath)
            meta.append(tmp)
    # snake style columns to load them to dbt
    columns = [inf.underscore(d.replace("UID","Uid").replace('ID', 'Id')) for d in config.columns]
	# append extra column for files
    out_cols = columns+['filename']
    df = pd.DataFrame(meta, columns=out_cols)
    if config.asset in ['devices', 'patients']:
        # drop filename column as `devices`, `patients` tables can have multiple entries
        # for now treat them as fact tables and provide source info for other tables
        df.drop(['filename'], axis=1, inplace=True)
        out_cols.pop()
    if config.partition_column is not None:
        df[config.partition_column] = pd.to_datetime(df[config.partition_column], format='%Y%m%d')
	# File indicates target type conversion we want to acheive, some values in pydicom
	# files will have `struct[]` types which are not easily shared across DuckDB and
	# Snowflake, so we need to do explicit conversion to `json[]` for further use in db
    anonymization_mapping = pd.read_csv(f"{DBT_PROJECT_NAME}/seeds/anonymization_mapping.csv")
    json_column_list = anonymization_mapping.loc[anonymization_mapping.target_type == "json[]", 'column_name'].values
    for column in df.columns:
        if column in json_column_list:
            df[column] = df[column].apply(lambda x: json.dumps(x))
    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

    out_cols.sort()
    return df[out_cols]

__all__ = [
    'COLUMN_PARTITION_MAP',
	'PARTITION_MONTHLY',
	'AssetExecutionContext',
	'AutoMaterializePolicy',
	'AssetIn',
	'RawConfig',
	'asset', 
	'os', 
	'pd', 
	'read_dicom_file_to_df', 
]

typemap = {
    pydicom.valuerep.DSdecimal: float,
    pydicom.valuerep.DSfloat: float,
    pydicom.valuerep.IS: int,
    pydicom.valuerep.PersonName: str,
    pydicom.multival.MultiValue: list,
    pydicom.uid.UID: str,
}

def cast(context: AssetExecutionContext, x):
    if isinstance(x, pydicom.sequence.Sequence):
        dicts = []
        for ds in x:
            if isinstance(ds, pydicom.dataset.Dataset):
                dicts.append(extract_struct(context, ds))
            else:
                dicts.append(ds)
        return dicts
    return typemap.get(type(x), lambda x: x)(x)


def extract_struct(
	context: AssetExecutionContext,
	dataset: pydicom.dataset.Dataset
) -> dict:
    """
    Recurcively extract data from DICOM dataset and put it
    into dictionary. Key are created from keyword, or if not
    defined from tag.

    Values are parced from DataElement values, multiple values
    are stored as list.
    Sequences are stored as lists of dictionaries

    Parameters
    ----------
    dataset: pydicom.dataset.Dataset
        dataset to extract

    Returns
    -------
    dict
    """
    res = dict()

    for el in dataset:
        key = el.keyword
        if key == '':
            key = str(el.tag)
        if el.VR == "SQ":
            res[key] = [extract_struct(context, val) for val in el]
        else:
            res[key] = DICOMtransform(context, el, clean=True)
    return res


def DICOMtransform(
	context: AssetExecutionContext,
	element: pydicom.dataelem.DataElement,
    clean: bool = False
):
    if element is None:
        return None
    VR = element.VR
    VM = element.VM
    val = element.value

    try:
        if VM > 1:
            return [decodeValue(val[i], VR, clean) for i in range(VM)]
        else:
            return decodeValue(val, VR, clean)

    except Exception as e:
        context.log.warning(f'Failed to decode value of type {VR} for: {e}')
        return None

def decodeValue(
	context: AssetExecutionContext,
	val,
	VR: str,
	clean=False
):
    """
    Decodes a value from pydicom.DataElement to corresponding
    python class following description in:
    http://dicom.nema.org/medical/dicom/current/output/
    chtml/part05/sect_6.2.html

    Nested values are not permitted

    Parameters
    ----------
    val:
        value as stored in DataElements.value
    VR: str
        Value representation defined by DICOM
    clean: bool
        if True, values of not-basic classes (None, int, float)
        transformed to string

    Returns
    -------
        decoded value
    """

    # Byte Numbers:
    # parced by pydicom, just return value
    if VR in ("FL", "FD",
              "SL", "SS", "SV",
              "UL", "US", "UV"):
        return val

    # Text Numbers
    # using int(), float()
    if VR == "DS":
        if val:
            return float(val)
        else:
            return None
    if VR == "IS":
        if val:
            return int(val)
        else:
            return None

    # Text and text-like
    # using strip to remove apddings
    if VR in ("AE", "CS",
              "LO", "LT",
              "SH", "ST", "UC",
              "UR", "UT", "UI"):
        return val.strip(" \0")

    # Persons Name
    # Use decoded original value
    if VR == "PN":
        if isinstance(val, str):
            return val.strip(" \0")
        if val == "":
            return None

        if len(val.encodings) > 0:
            enc = val.encodings[0]
            return val.original_string.decode(enc)
        else:
            context.log.warning("PN: unable to get encoding")
            return ""

    # Age string
    # unit mark is ignored, value converted to int
    if VR == "AS":
        if not val:
            return None
        if val[-1] in "YMWD":
            return int(val[:-1])
        else:
            return int(val)

    # Date and time
    # converted to corresponding datetime subclass
    if VR == "TM":
        if not val:
            return None
        if "." in val:
            dt = datetime.strptime(val, "%H%M%S.%f").time()
        else:
            dt = datetime.strptime(val, "%H%M%S").time()
        if clean:
            return dt.isoformat()
        else:
            return dt
    if VR == "DA":
        if not val:
            return None
        dt = datetime.strptime(val, "%Y%m%d").date()
        if clean:
            return dt.isoformat()
        else:
            return dt
    if VR == "DT":
        if not val:
            return None
        val = val.strip()
        date_string = "%Y%m%d"
        time_string = "%H%M%S"
        ms_string = ""
        uts_string = ""
        if "." in val:
            ms_string += ".%f"
        if "+" in val or "-" in val:
            uts_string += "%z"
        if len(val) == 8 or \
                (len(val) == 13 and uts_string != ""):
            context.log.warning(f"{val}: Format is DT, but string looks like DA")
            t = datetime.strptime(val, date_string + uts_string)
        elif len(val) == 6 or \
                (len(val) == 13 and ms_string != ""):
            context.log.warning(f"{val}: Format is DT, but string looks like TM")
            t = datetime.strptime(val, time_string + ms_string)
        else:
            t = datetime.strptime(val, date_string + time_string + ms_string + uts_string)
        if t.tzinfo is not None:
            t += t.tzinfo.utcoffset(t)
            t = t.replace(tzinfo=None)
        if clean:
            return t.isoformat()
        else:
            return t

    # Invalid type
    # Attributes and sequences will produce warning and return
    # None
    if VR in ("AT", "SQ", "UN"):
        context.log.warning(f"Invalid VR: {VR}")
        return None

    # Other type
    # Not clear how parce them
    if VR in ("OB", "OD", "OF", "OL", "OV", "OW"):
        return None

    # unregistered VR
    context.log.error(f"{VR} is not valid DICOM VR")
    raise ValueError(f"invalid VR: {VR}")
