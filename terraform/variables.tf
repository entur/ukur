variable "gcp_resources_project" {
  description = "The GCP pubsub project id"
}

variable "inbound_anshar_et_pubsub_topic" {
  description = "Topic to consume et messages from anshar"
}

variable "inbound_anshar_et_pubsub_subscription" {
  description = "Subscription name for inbound anshar et messages"
  default = "ukur.siri.estimated_timetables.subscription"
}

variable "inbound_anshar_sx_pubsub_topic" {
  description = "Topic to consume sx messages from anshar"
}

variable "inbound_anshar_sx_pubsub_subscription" {
  description = "Subscription name for inbound anshar sx messages"
  default = "ukur.siri.situation_exchange.subscription"
}