"""
This module specifies the Attributes that identify and describe general information about the Series
within a Study.
"""
columns = [
'BodyPartExamined',
'DeviceSerialNumber',
'FrameOfReferenceUID',
'Modality',
'PerformedProcedureStepDescription',
'PerformedProcedureStepID',
'PerformedProcedureStepStartDate',
'PerformedProcedureStepStartTime',
'PerformingPhysicianName',
'PositionReferenceIndicator',
'ProtocolName',
'ReferencedPerformedProcedureStepSequence',
'RelatedSeriesSequence',
'RequestAttributesSequence',
'SeriesDate',
'SeriesDescription',
'SeriesInstanceUID',
'SeriesNumber',
'SeriesTime',
'StudyDate',
'StudyID',
]

from .lib import *

@asset(
	metadata={"partition_expr": COLUMN_PARTITION_MAP["series"]},
	auto_materialize_policy=AutoMaterializePolicy.eager(),
	partitions_def=PARTITION_MONTHLY,
	compute_kind="pandas",
)
def series(
	context: AssetExecutionContext,
	config: RawConfig,
)-> pd.DataFrame:
    """
    Series within a Study.
    """
    return read_dicom_file_to_df(context, config)
