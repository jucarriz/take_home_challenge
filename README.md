# Aurelia Data Platform

> Take-home challenge — data platform for a real-time payments fintech.
> Batch pipeline orchestrated with Airflow that converts raw payment events
> into reliable business metrics (auth rate, chargeback rate, cohort LTV,
> fraud signals) using the medallion architecture (bronze / silver / gold).

## Status

Work in progress. This README will be filled in as the implementation
advances. The final version follows this checklist:

- [ ] Features (what the pipeline produces)
- [ ] Prerequisites (Docker Desktop, free ports, RAM)
- [ ] How to run (single command)
- [ ] How to run tests
- [ ] Repo structure
- [ ] Data model (link to `ARCHITECTURE.md`)
- [ ] Decisions and trade-offs
- [ ] Areas to improve / future work (Kafka streaming, Terraform, dbt on gold)
- [ ] Env vars (reference to `.env.example`)
- [ ] Screenshots (Airflow DAG green, MinIO buckets, DBeaver on gold)

## Quick links

- Business/technical plan: [plan_definitivo.md](plan_definitivo.md)
- Reference repo analysis (professor's NestJS repo): [analisis_repo_profesor.md](analisis_repo_profesor.md)
- Challenge brief: [take_home_challenge_data_engineer_aurelia.pdf](take_home_challenge_data_engineer_aurelia.pdf)
