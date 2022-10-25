terraform {
  required_version = ">= 0.13.2"
}

provider "google" {
  version = ">= 4.26"
}

resource "google_pubsub_subscription" "anshar_et_subscription" {
  project = var.gcp_resources_project
  name = var.inbound_anshar_et_pubsub_subscription
  topic = var.inbound_anshar_et_pubsub_topic
}

resource "google_pubsub_subscription" "anshar_sx_subscription" {
  project = var.gcp_resources_project
  name = var.inbound_anshar_sx_pubsub_subscription
  topic = var.inbound_anshar_sx_pubsub_topic
}



