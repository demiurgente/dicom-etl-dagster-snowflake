"""
This module specifies the Attributes that identify and describe the piece of equipment that produced
a Series of Composite Instances.
"""
columns = [
'DeviceSerialNumber',
'InstitutionAddress',
'InstitutionalDepartmentName',
'InstitutionName',
'Manufacturer',
'ManufacturerModelName',
'SoftwareVersions',
'StationName',
]

from .lib import *

@asset(
	auto_materialize_policy=AutoMaterializePolicy.eager(),
    compute_kind="pandas",
)
def devices(
	context: AssetExecutionContext,
	config: RawConfig,
)-> pd.DataFrame:
    """
	Devices or calibration objects (e.g., catheters, markers, baskets) from a Study and/or image.
	"""
    return read_dicom_file_to_df(context, config)
