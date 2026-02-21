resource "aws_s3_bucket" "raw" {
    bucket = var.nombre_bucket_raw
    tags = {
        name = "S3_bucket"
        owner = "Diego"
        team = "Unico"
        proyecto = "Chess"
    }
}

resource "aws_s3_bucket" "cleaned" {
    bucket = var.nombre_bucket_cleaned
    tags = {
        name = "S3_bucket"
        owner = "Diego"
        team = "Unico"
        proyecto = "Chess"
    }
}

resource "aws_glue_catalog_database" "chess_db" {
  name = "chess_analytics"
}

resource "aws_glue_catalog_table" "gold_stats_country" {
  name          = "gold_stats_country"
  database_name = aws_glue_catalog_database.chess_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.nombre_bucket_cleaned}/gold/stats_by_country/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "avg_blitz_rating"
      type = "double"
    }
    columns {
      name = "avg_rapid_rating"
      type = "double"
    }
    columns {
      name = "total_players"
      type = "bigint"
    }
    columns {
      name = "count_GM"
      type = "bigint"
    }
    columns {
      name = "count_IM"
      type = "bigint"
    }
    columns {
      name = "count_FM"
      type = "bigint"
    }
    columns {
      name = "count_CM"
      type = "bigint"
    }
    columns {
      name = "count_NM"
      type = "bigint"
    }
    columns {
      name = "count_WGM"
      type = "bigint"
    }
    columns {
      name = "count_WIM"
      type = "bigint"
    }
    columns {
      name = "count_WFM"
      type = "bigint"
    }
    columns {
      name = "count_WCM"
      type = "bigint"
    }
    columns {
      name = "count_WNM"
      type = "bigint"
    }
    columns {
      name = "top_player_name"
      type = "string"
    }
    columns {
      name = "top_player_elo"
      type = "int"
    }
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "s3-bucket-chess-athena-results-diego"
  force_destroy = true
}

resource "aws_athena_workgroup" "powerbi_workgroup" {
  name = "powerbi_analytics"

  configuration {
    enforce_workgroup_configuration    = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }
  }
}