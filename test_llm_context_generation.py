from app.dbt_loader import fetch_manifest_json
from app.model_extractor import (
    get_team_model_summary,
    get_relevant_models_from_question,
    format_prompt_context
)

# Define your team model list
team_model_list = [
    "stg_worker_ops_agent_availabilities",
    "stg_worker_ops__autoqa_conversations",
    "stg_worker_ops__wfm_agent_roster",
    "stg_worker_ops__wfm_forecast_data",
    "stg_worker_ops__wfm_tymeshift_schedules",
    "stg_worker_ops__wfm_zendesk_tickets_data",
    "fct_zendesk__agents_productivity",
    "fct_zendesk__docs_qa_tickets",
    "fct_zendesk__hcf_cx_tickets",
    "fct_zendesk__hcf_qa_tickets",
    "fct_zendesk__help_center_articles",
    "fct_zendesk__mqr_appeals_tickets",
    "fct_zendesk__mqr_audit_the_auditor_tickets",
    "fct_zendesk__mqr_consult_tickets",
    "fct_zendesk__mqr_qa_tickets",
    "fct_zendesk__payments_tickets",
    "fct_zendesk__techops_tickets",
    "fct_zendesk__ticket_comments",
    "fct_zendesk__ticket_events",
    "fct_zendesk__wops_escalations_tickets",
    "fct_zendesk_wops__docs_tickets",
    "fct_zendesk_wops__qa_tickets",
    "fct_zendesk_wops__tickets",
    "fct_zendesk_tickets",
    "fct_zendesk__techsupport_tickets",
    "fct_zendesk__mqr_tickets",
    "fct_zendesk__docs_tickets",
    "dim_zendesk_wops__users",
    "dim_zendesk_users",
    "dim_zendesk_organizations",
    "fct_wfm_airtable__team_details",
    "fct_survey_sparrow_question_responses",
    "fct_mqr__schedule_adherence",
    "fct_klaus__reviews",
    "fct_360_learning__user_paths",
    "fct_amazon_connect__agent_metrics",
    "fct_amazon_connect__queue_metrics",
    "fct_nice_reply_survey_answers",
    "stg_kops_airtable__sop_freshness_list",
    "stg_kops_airtable__requests_backlog",
    "stg_kops_airtable__feq_build_status",
    "stg_wops_google_analytics__pages"
]

# Step 1: Fetch manifest
manifest = fetch_manifest_json()

# Step 2: Extract team-specific summary
summary = get_team_model_summary(manifest, team_model_list)

# Step 3: Ask a test question
question = "how many tickets were created in the Voice channel in the last 7 days?"
top_models = get_relevant_models_from_question(question, summary)
context = format_prompt_context(summary, top_models)

# Step 4: Print result
print("🔎 Matched models:", top_models)
print("\n📄 LLM Prompt Context:\n")
print(context)