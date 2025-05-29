# 📘 README: Monthly Home and Work Location Detection from Mobile Connections

## Overview

This pipeline processes anonymized mobile phone connection data to determine **monthly home and work locations** for users. It applies weighted logic to time-of-day activity to infer likely home and work sites, using only data from a specific region.

---

## Tables Created

### `xdr.sanhel_metroregion`

- **Description:**  
  A filtered subset of mobile connection data, limited to a specific administrative region (code 13).

- **Source Tables:**
  - `xdr.2023`: Anonymized mobile connection logs with fields such as `user_id`, `cell_id`, `timestamp`.
  - `xdr.cells2023`: Metadata mapping each `cell_id` to its region and a generalized `site_id`.

- **Logic:**
  - Joins connection logs (`cell_id`) with tower metadata.
  - Keeps only records where the cell belongs to region 13.

---

### `xdr.sanhel_home_work`

- **Description:**  
  Infers each user's **home** and **work** locations by month, based on weighted connection activity at different times of day.

#### Home Detection Logic

- Uses nighttime and early morning hours (00:00–06:00 and 23:00).
- Assigns highest weights to 02:00–03:59.
- Aggregates total weight per user/site/month.
- Selects the site with the highest weight as `home_site_id`.

#### Work Detection Logic

- Uses typical weekday work hours (09:00–17:59).
- Excludes weekends.
- Assigns higher weights to 09:00–11:59 and 14:00–17:59.
- Aggregates and selects `work_site_id` per user/month similarly.

#### Output Columns

- `user_id`
- `month`
- `home_site_id`, `home_weight`
- `work_site_id`, `work_weight`

---

## Notes

- All time-based rules assume timestamps are localized.
- `site_id` is an abstracted representation of physical cell towers.
- Only users with detectable activity in the selected region (code 13) are considered.

---
