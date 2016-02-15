-- Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License. You may obtain a copy of the
-- License at  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.


CALL create_index_if_not_exists(null, 'gobblin_job_metrics', 'ux_job_metric', 'unique', '(job_id, metric_group, metric_name, metric_type)');
CALL drop_index_if_exists(null, 'gobblin_job_metrics', 'metric_group');
CALL drop_index_if_exists(null, 'gobblin_job_metrics', 'metric_name');
CALL drop_index_if_exists(null, 'gobblin_job_metrics', 'metric_type');

CALL create_index_if_not_exists(null, 'gobblin_task_metrics', 'ux_task_metric', 'unique', '(task_id, metric_group, metric_name, metric_type)');
CALL drop_index_if_exists(null, 'gobblin_task_metrics', 'metric_group');
CALL drop_index_if_exists(null, 'gobblin_task_metrics', 'metric_name');
CALL drop_index_if_exists(null, 'gobblin_task_metrics', 'metric_type');

CALL create_index_if_not_exists(null, 'gobblin_job_properties', 'ux_job_property', 'unique', '(job_id, property_key)');
CALL drop_index_if_exists(null, 'gobblin_job_properties', 'property_key');

CALL create_index_if_not_exists(null, 'gobblin_task_properties', 'ux_task_property', 'unique', '(task_id, property_key)');
CALL drop_index_if_exists(null, 'gobblin_task_properties', 'property_key');
