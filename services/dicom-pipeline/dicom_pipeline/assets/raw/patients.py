"""
This module specifies the Attributes of the Patient that describe and identify the Patient
who is the subject of a Study. This Module contains Attributes of the Patient that are needed
for interpretation of the Composite Instances and are common for all Studies performed on the
Patient. It contains Attributes that are also included in the Patient Modules in Section C.2.
"""

columns = [
'Allergies',
'BranchOfService',
'CountryOfResidence',
'EthnicGroup',
'IssuerOfPatientID',
'MilitaryRank',
'Occupation',
'OtherPatientIDs',
'OtherPatientNames',
'PatientAddress',
'PatientAge',
'PatientBirthDate',
'PatientBirthName',
'PatientBirthTime',
'PatientComments',
'PatientID',
'PatientMotherBirthName',
'PatientName',
'PatientReligiousPreference',
'PatientSex',
'PatientSize',
'PatientTelephoneNumbers',
'PatientWeight',
'RegionOfResidence',
'SmokingStatus',
'SpecialNeeds',
]

from .lib import *

@asset(
	auto_materialize_policy=AutoMaterializePolicy.eager(),
	compute_kind="pandas",
)
def patients(
	context: AssetExecutionContext,
	config: RawConfig,
)-> pd.DataFrame:
    """
    Attiributes for a Subject of a Study.
    """
    return read_dicom_file_to_df(context, config)
