# Image Compression from DICOM files

This project contains a service that allows converting raw pixel data into an image with a specified type. The main use case is to download raw **DICOM** files from **S3**, iterate over a list of **DICOM** files, convert them to JPEG images, and upload them to the remote **S3** bucket location. Supports built-in parallel execution with `rayon` which will map conversion jobs onto the available number of CPUs. It is useful to quickly prototype an image converter that would serve as a dynamic compression routine. The project is organised to be deployed to **AWS Lambda** and used as a remote processing unit

## Core Functionality

So this would run in parallel on available CPUs:

- Download a file from **S3**
- Compress pixel data into a JPEG image with original resolution
- Upload the resulting image back to **S3**

## Config

In order to launch a Lambda job you would need to provide information corresponding to the following:

`s3://source_bucket/source_prefix`

- `source_bucket` - source S3 bucket to pull files from
- `source_prefix` - prefix leading to the folder with DICOM files

`s3://source_bucket/source_prefix/*`

- `files` - list of dicom files as keys on s3 without prefixing our bucket

`s3://target_bucket/target_prefix`

- `target_bucket` - target S3 bucket to push files to
- `target_prefix` - future folder for compressed image

## Image Compression Presets

The current setting is hardcoded to `Luma8`` grayscale and JPEG format for compression
