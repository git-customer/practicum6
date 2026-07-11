resource "yandex_dataproc_cluster" "hadoop-cluster" {
  autoscaling_service_account_id = "ajepto4590qa06tq2m5t"
  cluster_config {
    version_id = "2.1"
    hadoop {
      services = [
        "YARN",
        "MAPREDUCE",
        "HDFS"
      ]
      properties = {
      }
      ssh_public_keys = [
        "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDfu9E61Ei1ewcsNTCNGdu8Re9hZuGqmdfqlqWFsOHcxj37OlTuUCJpraXrWENQNZmgtFdv84Th5uHCl+8Ws6eakpezd5I3ttFCvo5I9FtO3CIXuT3K0gBMb3LmOaBZlI3KSvo2ZwESBTdbuwzdewa0eNCu86XYnAkq0KaBX04qRMb22Fi7U7kFR0nb654jwEqguWL9Oat/yrhemthKfPUPq32Z+kvdeK/mpulS2/Bgj2aZt8dzQINxubWEDpwjbUg+svhS3MKvr9rG9+mYgUDuw7r2YkSXfVbR1cjiUutj45FWm3SZ1GaiHxwvCcxw0l5YCv+W7NvCh8H+8vjeywCD"
      ]
      oslogin = false
    }
    subcluster_spec {
      name        = "dataproc721_subcluster858"
      role        = "MASTERNODE"
      hosts_count = 1
      subnet_id   = "fl8k0ed528tqrfja23vf"
      resources {
        disk_size          = 20
        disk_type_id       = "network-ssd"
        resource_preset_id = "s3-c2-m8"
      }
    }
    subcluster_spec {
      name        = "dataproc721_subcluster632"
      role        = "DATANODE"
      hosts_count = 1
      subnet_id   = "fl8k0ed528tqrfja23vf"
      resources {
        disk_size          = 20
        disk_type_id       = "network-ssd"
        resource_preset_id = "s3-c2-m8"
      }
    }
  }
  folder_id          = "b1gc16aa090nk1a159kt"
  name               = "hadoop-cluster"
  service_account_id = "ajepto4590qa06tq2m5t"
  ui_proxy           = true
  zone_id            = "ru-central1-d"
}
