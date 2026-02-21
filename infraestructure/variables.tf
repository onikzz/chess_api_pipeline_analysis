variable "region" {
    description = "Region del aws"
    default = "us-west-2"
}

variable "nombre_bucket_raw" {
    description = "Nombre para un bucket S3"
    default = "s3-bucket-chess-proyecto-raw"
}

variable "nombre_bucket_cleaned" {
    description = "Nombre para un bucket S3"
    default = "s3-bucket-chess-proyecto-cleaned"
}

variable "nombre_bucket_results" {
    default = "s3-bucket-chess-athena-results-diego"
}