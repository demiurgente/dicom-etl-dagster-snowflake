"""
This module defines Attributes that provide information about the Patient at the time the Study started.
"""

columns = [
'AccessionNumber',
'AdditionalPatientHistory',
'AdmittingDiagnosesDescription',
'LastMenstrualDate',
'MedicalAlerts',
'MedicalRecordLocator',
'NameOfPhysiciansReadingStudy',
'PatientID',
'PatientState',
'PhysiciansOfRecord',
'ReferringPhysicianName',
'RequestedProcedureDescription',
'RequestingPhysician',
'RequestingService',
'ScheduledStudyStartDate',
'ScheduledStudyStartTime',
'SeriesDate',
'SpecialNeeds',
'StudyComments',
'StudyDate',
'StudyDescription',
'StudyID',
'StudyInstanceUID',
'StudyTime',
]

from .lib import *

@asset(
	metadata={"partition_expr": COLUMN_PARTITION_MAP["studies"]},
	auto_materialize_policy=AutoMaterializePolicy.eager(),
	partitions_def=PARTITION_MONTHLY,
	compute_kind="pandas",
)
def studies(
	context: AssetExecutionContext,
	config: RawConfig,
)-> pd.DataFrame:
    """
    Attributes of the Study performed upon the Patient.
    """
    return read_dicom_file_to_df(context, config)
