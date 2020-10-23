terraform {
  required_version = ">= 0.12"
}

provider "google" {
  version = "~> 2.19"
}

provider "kubernetes" {
  load_config_file = var.load_config_file
}

resource "google_service_account" "service_account" {
  account_id   = "${var.labels.team}-${var.labels.app}-sa"
  display_name = "${var.labels.team}-${var.labels.app} service account"
  project = var.gcp_project
}

resource "google_service_account_key" "service_account_key" {
  service_account_id = google_service_account.service_account.name
}

resource "kubernetes_secret" "service_account_credentials" {
  metadata {
    name      = "${var.labels.team}-${var.labels.app}-sa-key"
    namespace = var.kube_namespace
  }
  data = {
    "credentials.json" = base64decode(google_service_account_key.service_account_key.private_key)
  }
}

resource "google_pubsub_subscription" "anshar_et_subscription" {
  project = var.pubsub_project
  name = var.inbound_anshar_et_pubsub_subscription
  topic = var.inbound_anshar_et_pubsub_topic
}

resource "google_pubsub_subscription_iam_member" "anshar_et_subscription_iam_member" {
  project = var.pubsub_project
  subscription = google_pubsub_subscription.anshar_et_subscription.name
  role = var.inbound_anshar_et_subscription_role
  member = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_pubsub_subscription" "anshar_sx_subscription" {
  project = var.pubsub_project
  name = var.inbound_anshar_sx_pubsub_subscription
  topic = var.inbound_anshar_sx_pubsub_topic
}

resource "google_pubsub_subscription_iam_member" "anshar_sx_subscription_iam_member" {
  project = var.pubsub_project
  subscription = google_pubsub_subscription.anshar_sx_subscription.name
  role = var.inbound_anshar_sx_subscription_role
  member = "serviceAccount:${google_service_account.service_account.email}"
}

# add service account as member to the datastore
resource "google_project_iam_member" "project" {
  project = var.gcp_project
  role    = var.service_account_datastore_role
  member = "serviceAccount:${google_service_account.service_account.email}"
}