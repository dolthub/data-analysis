#!/usr/bin/env fish

if test (count $argv) -ne 1
    echo "Usage: view_pr.fish <pull_request_number>"
    exit 1
end

function add_owner_if_not_exists
    dolt remote add $owner https://doltremoteapi.dolthub.com/$owner/transparency-in-pricing 2>/dev/null
end

function check_urls
    echo "Checking the list of URLs from the diff"

    dolt sql -q "select \
    to_mrf_url as mrg_url \
    from dolt_diff('$merge_base', '$owner/$branch', 'hospital')" -r csv | python3 check_urls.py

    read -l -P "Press ENTER to continue"
end


function check_missing_ids
    echo "Checking for ids which are in the hospital table but not the rate table..."

    set query "
        SELECT dolt_diff_1.to_id
        FROM dolt_diff('$merge_base', '$owner/$branch', 'hospital')
        LEFT JOIN (
            SELECT DISTINCT to_hospital_id
            FROM dolt_diff('$merge_base', '$owner/$branch', 'rate')
        ) AS r
        ON dolt_diff_1.to_id = r.to_hospital_id
        WHERE r.to_hospital_id IS NULL;
    "
    dolt sql -q "$query"
    read -l -P "Press ENTER to continue"
end

function view_hospital_table
    set query "select \
    `to_id` as `id`, \
    `to_ein` as `ein`, \
    `to_file_name` as `file_name`, \
    `to_name` as `name`, \
    `to_alt_name` as `alt_name`, \
    `to_system_name` as `system_name`, \
    `to_addr` as `addr`, \
    `to_city` as `city`, \
    `to_state` as `state`, \
    `to_last_updated` as `last_updated`, \
    `to_transparency_page` as `transparency_page`, \
    `to_additional_notes` as `additional_notes`, \
    `to_mrf_url` as `mrf_url`, \
    diff_type \
    from dolt_diff('$merge_base', '$owner/$branch', 'hospital')"

    dolt sql -q "$query" -r csv | vd -f csv --play cmdlog.vdj
end


function view_rate_table
    set query "select \
                dolt_diff_1.to_hospital_id, \
                dolt_diff_1.to_line_type, \
                dolt_diff_1.to_description, \
                dolt_diff_1.to_rev_code, \
                dolt_diff_1.to_local_code, \
                dolt_diff_1.to_code, \
                dolt_diff_1.to_ms_drg, \
                dolt_diff_1.to_apr_drg, \
                dolt_diff_1.to_eapg, \
                dolt_diff_1.to_hcpcs_cpt, \
                dolt_diff_1.to_modifiers, \
                dolt_diff_1.to_alt_hcpcs_cpt, \
                dolt_diff_1.to_thru, \
                dolt_diff_1.to_apc, \
                dolt_diff_1.to_icd, \
                dolt_diff_1.to_ndc, \
                dolt_diff_1.to_drug_hcpcs_multiplier, \
                dolt_diff_1.to_drug_quantity, \
                dolt_diff_1.to_drug_unit_of_measurement, \
                dolt_diff_1.to_drug_type_of_measurement, \
                dolt_diff_1.to_billing_class, \
                dolt_diff_1.to_setting, \
                dolt_diff_1.to_payer_category, \
                dolt_diff_1.to_payer_name, \
                dolt_diff_1.to_plan_name, \
                dolt_diff_1.to_standard_charge, \
                dolt_diff_1.to_standard_charge_percent, \
                dolt_diff_1.to_contracting_method, \
                dolt_diff_1.to_additional_generic_notes, \
                dolt_diff_1.to_additional_payer_specific_notes, \
                dolt_diff_1.diff_type, \
                dolt_diff_1.to_row_id \
                from dolt_diff('$merge_base', '$owner/$branch', 'rate') \
                where dolt_diff_1.to_row_id < 1844674407370955161
        "
    dolt sql -q "$query" -r csv | vd  -f csv
end

set pr_number $argv[1]

echo "* Fetching branch details"

set output (python3 fetch_branch_details.py --pr $pr_number)

set -l owner_branch (string split " " -- $output)
set owner $owner_branch[1]
set branch $owner_branch[2]

add_owner_if_not_exists

echo "* Fetching commits from $owner"

dolt fetch $owner

echo "* Finding merge base with $owner/$branch"

set merge_base (dolt merge-base main $owner/$branch)

check_urls

view_hospital_table

check_missing_ids

view_rate_table
