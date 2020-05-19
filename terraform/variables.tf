variable "gcp_project" {
  description = "The GCP project id"
}

variable "kube_namespace" {
  description = "The Kubernetes namespace"
}

variable "load_config_file" {
  description = "Do not load kube config file"
  default     = false
}

variable "labels" {
  description = "Labels used in all resources"
  type        = map(string)
  default = {
    manager = "terraform"
    team    = "ror"
    slack   = "talk-ror"
    app     = "ukur"
  }
}

variable "inbound_anshar_et_pubsub_topic" {
  description = "Topic to consume et messages from anshar"
  default = "protobuf.estimated_timetables"
}

variable "inbound_anshar_et_pubsub_subscription" {
  description = "Subscription name for inbound anshar et messages"
  default = "ukur.protobuf.estimated_timetables"
}

variable "inbound_anshar_et_subscription_role" {
  description = "IAM role for inboud anshar et subscription"
  default = "roles/reader"
}
