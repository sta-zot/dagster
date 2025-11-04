CREATE TABLE "fact_events" (
  "fact_event_id" SERIAL PRIMARY KEY,
  "is_edu_materials_used" bool,
  "auditorium" varchar,
  "event_name" varchar,
  "location_id" int,
  "partner_id" int,
  "event_id" int,
  "audience_id" int,
  "organizer_id" int,
  "date_id" int,
  "participants_cnt" int,
  "volunteers_cnt" int
);

CREATE TABLE "volunteers_in_events" (
  "id" SERIAL PRIMARY KEY,
  "volunteer_id" int,
  "fact_event_id" int,
  "comment" varchar
);

CREATE TABLE "fact_trainings" (
  "id" SERIAL PRIMARY KEY,
  "study_mode" varchar,
  "location_id" int,
  "training_provider_id" int,
  "training_program_id" int,
  "staff_id" int,
  "affiliation_org_id" int,
  "start_date_id" int,
  "end_date_id" int
);

CREATE TABLE "fact_edu_integrations" (
  "id" SERIAL PRIMARY KEY,
  "has_fin_lit" bool,
  "location_id" int,
  "organization_id" int,
  "edu_program_id" int,
  "date_id" int,
  "teachers_cnt" int,
  "teachers_finlit_trained_cnt" int,
  "teachers_finlit_train_cnt" int,
  "students_cnt" int,
  "students_with_finlit_cnt" int
);

CREATE TABLE "fact_im_placements" (
  "id" SERIAL PRIMARY KEY,
  "url" text,
  "location_id" int,
  "info_mat_id" int,
  "placement_point_id" int,
  "partner_id" int,
  "audience_id" int,
  "organizer_id" int,
  "date_id" int,
  "views_cnt" int,
  "posted_materials_cnt" int
);

CREATE TABLE "dim_edu_program" (
  "edu_program_id" SERIAL PRIMARY KEY,
  "edu_program_bk" int UNIQUE,
  "name" varchar,
  "type" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to"  DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_staff" (
  "staff_id" SERIAL PRIMARY KEY,
  "staff_bk" int,
  "name" varchar,
  "type" varchar,
  "spec" varchar,
  "department" varchar,
  "organization" varchar,
  "is_fin_lit_provider" bool,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_organization" (
  "organization_id" SERIAL PRIMARY KEY,
  "organization_bk" int,
  "name" varchar,
  "type" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_date" (
  "date_id" int PRIMARY KEY,
  "date" date,
  "year" int,
  "month" int,
  "day_of_month" int,
  "day_of_week" int,
  "quarter" int
);

CREATE TABLE "dim_training_program" (
  "training_program_id" SERIAL PRIMARY KEY,
  "training_program_bk" int UNIQUE,
  "name" varchar,
  "provider" varchar,
  "duration_hours" float,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_info_materials" (
  "info_materials_id" SERIAL PRIMARY KEY,
  "info_materials_bk" int,
  "name" varchar,
  "type" varchar,
  "format" varchar,
  "topic" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_placement_point" (
  "placement_point_id" SERIAL PRIMARY KEY,
  "placement_point_bk" int,
  "name" varchar,
  "type" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_location" (
  "location_id" SERIAL PRIMARY KEY,
  "oktmo" varchar,
  "region" varchar,
  "municipality" varchar,
  "settlement" varchar,
  "type" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_event" (
  "event_id" SERIAL PRIMARY KEY,
  "event_bk" int,
  "type" varchar,
  "format" varchar,
  "topic" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_partner" (
  "partner_id" SERIAL PRIMARY KEY,
  "partner_bk" int,
  "type" varchar,
  "name" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

CREATE TABLE "dim_audience" (
  "audience_id" SERIAL PRIMARY KEY,
  "age_group" varchar,
  "soc_group" varchar,
  "valid_from"  DATE DEFAULT (now()),
  "valid_to" DATE DEFAULT (CURRENT_DATE + INTERVAL '100 years'),
  "is_current" bool
);

ALTER TABLE "fact_events" ADD FOREIGN KEY ("location_id") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("partner_id") REFERENCES "dim_partner" ("partner_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("event_id") REFERENCES "dim_event" ("event_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("audience_id") REFERENCES "dim_audience" ("audience_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("organizer_id") REFERENCES "dim_staff" ("staff_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("date_id") REFERENCES "dim_date" ("date_id");

ALTER TABLE "volunteers_in_events" ADD FOREIGN KEY ("fact_event_id") REFERENCES "fact_events" ("fact_event_id");

ALTER TABLE "volunteers_in_events" ADD FOREIGN KEY ("volunteer_id") REFERENCES "dim_staff" ("staff_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("location_id") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("affiliation_org_id") REFERENCES "dim_organization" ("organization_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("training_provider_id") REFERENCES "dim_organization" ("organization_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("training_program_id") REFERENCES "dim_training_program" ("training_program_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("staff_id") REFERENCES "dim_staff" ("staff_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("start_date_id") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_trainings" ADD FOREIGN KEY ("end_date_id") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_edu_integrations" ADD FOREIGN KEY ("location_id") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fact_edu_integrations" ADD FOREIGN KEY ("organization_id") REFERENCES "dim_organization" ("organization_id");

ALTER TABLE "fact_edu_integrations" ADD FOREIGN KEY ("edu_program_id") REFERENCES "dim_edu_program" ("edu_program_id");

ALTER TABLE "fact_edu_integrations" ADD FOREIGN KEY ("date_id") REFERENCES "dim_date" ("date_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("location_id") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("info_mat_id") REFERENCES "dim_info_materials" ("info_materials_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("placement_point_id") REFERENCES "dim_placement_point" ("placement_point_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("partner_id") REFERENCES "dim_partner" ("partner_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("audience_id") REFERENCES "dim_audience" ("audience_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("organizer_id") REFERENCES "dim_staff" ("staff_id");

ALTER TABLE "fact_im_placements" ADD FOREIGN KEY ("date_id") REFERENCES "dim_date" ("date_id");

CREATE UNIQUE INDEX "program_idx" ON "dim_edu_program" ("name", "type");

CREATE UNIQUE INDEX "staf_idx" ON "dim_staff" ("name", "type", "spec", "department");

CREATE UNIQUE INDEX "org_idx" ON "dim_organization" ("name", "type");

CREATE UNIQUE INDEX "training_idx" ON "dim_training_program" ("name", "provider", "duration_hours");

CREATE UNIQUE INDEX "im_idx" ON "dim_info_materials" ("name", "type", "topic", "format");

CREATE UNIQUE INDEX "placement_idx" ON "dim_placement_point" ("name", "type");

CREATE UNIQUE INDEX "event_idx" ON "dim_event" ("type", "format", "topic");

CREATE UNIQUE INDEX "partner_idx" ON "dim_partner" ("name", "type");

CREATE UNIQUE INDEX "audience_idx" ON "dim_audience" ("age_group", "soc_group");
