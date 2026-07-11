resource "yandex_compute_instance" "practicum-vm" {
  boot_disk {
    initialize_params {
      name       = "disk-ubuntu-24-04-lts-1783675482203"
      type       = "network-ssd"
      size       = 20
      block_size = 4096
      image_id   = "fd8dcjve5vsdhbqs6nqj"
    }
    auto_delete = true
  }
  folder_id          = "b1gc16aa090nk1a159kt"
  hostname           = "practicum-vm"
  maintenance_policy = "MAINTENANCE_POLICY_UNSPECIFIED"
  metadata = {
    user-data               = "#cloud-config\ndatasource:\n Ec2:\n  strict_id: false\nssh_pwauth: no\nusers:\n- name: vm-user\n  sudo: ALL=(ALL) NOPASSWD:ALL\n  shell: /bin/bash\n  ssh_authorized_keys:\n  - ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDfu9E61Ei1ewcsNTCNGdu8Re9hZuGqmdfqlqWFsOHcxj37OlTuUCJpraXrWENQNZmgtFdv84Th5uHCl+8Ws6eakpezd5I3ttFCvo5I9FtO3CIXuT3K0gBMb3LmOaBZlI3KSvo2ZwESBTdbuwzdewa0eNCu86XYnAkq0KaBX04qRMb22Fi7U7kFR0nb654jwEqguWL9Oat/yrhemthKfPUPq32Z+kvdeK/mpulS2/Bgj2aZt8dzQINxubWEDpwjbUg+svhS3MKvr9rG9+mYgUDuw7r2YkSXfVbR1cjiUutj45FWm3SZ1GaiHxwvCcxw0l5YCv+W7NvCh8H+8vjeywCD"
    ssh-keys                = "vm-user:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDfu9E61Ei1ewcsNTCNGdu8Re9hZuGqmdfqlqWFsOHcxj37OlTuUCJpraXrWENQNZmgtFdv84Th5uHCl+8Ws6eakpezd5I3ttFCvo5I9FtO3CIXuT3K0gBMb3LmOaBZlI3KSvo2ZwESBTdbuwzdewa0eNCu86XYnAkq0KaBX04qRMb22Fi7U7kFR0nb654jwEqguWL9Oat/yrhemthKfPUPq32Z+kvdeK/mpulS2/Bgj2aZt8dzQINxubWEDpwjbUg+svhS3MKvr9rG9+mYgUDuw7r2YkSXfVbR1cjiUutj45FWm3SZ1GaiHxwvCcxw0l5YCv+W7NvCh8H+8vjeywCD"
    private_ui_created_from = "console"
  }
  name = "practicum-vm"
  network_interface {
    subnet_id = "fl8k0ed528tqrfja23vf"
    index     = 0
    nat       = true
  }
  platform_id = "standard-v3"
  resources {
    memory        = 4
    cores         = 2
    core_fraction = 20
  }
  scheduling_policy {
    preemptible = false
  }
  zone = "ru-central1-d"
}
