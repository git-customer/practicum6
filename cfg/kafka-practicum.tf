resource "yandex_mdb_kafka_cluster" "kafka-practicum" {
  config {
    version = "3.9"
    zones = [
      "ru-central1-b",
      "ru-central1-d",
      "ru-central1-a"
    ]
    brokers_count    = 1
    assign_public_ip = true
    schema_registry  = true
    access {
      data_transfer = false
    }
    kafka_ui {
      enabled = false
    }
    disk_size_autoscaling {
      disk_size_limit           = 320
      emergency_usage_threshold = 90
      planned_usage_threshold   = 80
    }
    kafka {
      resources {
        disk_size          = 32
        disk_type_id       = "network-ssd"
        resource_preset_id = "s4a-c2-m8"
      }
      kafka_config {
        log_retention_ms  = "86400000"
        log_segment_bytes = "10485760"
      }
    }
    zookeeper {
      resources {
        disk_size          = 10
        disk_type_id       = "network-ssd"
        resource_preset_id = "s4a-c2-m8"
      }
    }
  }
  deletion_protection = false
  environment         = "PRODUCTION"
  folder_id           = "b1gc16aa090nk1a159kt"
  maintenance_window {
    type = "WEEKLY"
    day  = "SAT"
    hour = 24
  }
  name       = "kafka-practicum"
  network_id = "enpnb3lc8mcapjqcro6u"
  subnet_ids = [
    "e9bq9d7hcg6f56rn4ljj",
    "e2lem5vkufmdkk5avlug",
    "fl8k0ed528tqrfja23vf",
    "ajc8at938nvptsq2aa18"
  ]
}
