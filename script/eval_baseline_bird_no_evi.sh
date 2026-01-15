print_yellow() {
  local text=$1
  echo -e "\033[1;33m${text}\033[0m"
}
# BIRD-FinC
# with evidence
# Zero-shot (GPT-4o)
# BIRD-FinC (Original)

print_yellow "Baseline of BIRD. no_evidence"


print_yellow "Zero-shot (GPT-4o)"
print_yellow "BIRD-FinC (Original)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_original.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_FinancialCorrected-orignal.json \
    --diff_json_path dataset/bird-FinC/financial.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_original.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_FinancialCorrected-orignal.json \
    --diff_json_path dataset/bird-FinC/financial.json \
    --num_cpus $(nproc)

# BIRD-FinC (SQL)
print_yellow "BIRD-FinC (SQL)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_sql.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_FinancialCorrectedSQL.json \
    --diff_json_path dataset/bird-FinC/financial_corrected_sql.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_sql.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_FinancialCorrectedSQL.json \
    --diff_json_path dataset/bird-FinC/financial_corrected_sql.json \
    --num_cpus $(nproc)

# BIRD-FinC (Data)
print_yellow "BIRD-FinC (Data)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_data.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_FinancialCorrected.json \
    --diff_json_path dataset/bird-FinC/financial_corrected.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_data.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_FinancialCorrected.json \
    --diff_json_path dataset/bird-FinC/financial_corrected.json \
    --num_cpus $(nproc)

# DIN-SQL (GPT-4o)
print_yellow "DIN-SQL"
print_yellow "BIRD-FinC (Original)"

echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_original.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_FinancialCorrected-orignal.json \
    --diff_json_path dataset/bird-FinC/financial.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_original.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_FinancialCorrected-orignal.json \
    --diff_json_path dataset/bird-FinC/financial.json \
    --num_cpus $(nproc)

# BIRD-FinC (SQL)
print_yellow "BIRD-FinC (SQL)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_sql.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_FinancialCorrectedSQL.json \
    --diff_json_path dataset/bird-FinC/financial_corrected_sql.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_sql.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_FinancialCorrectedSQL.json \
    --diff_json_path dataset/bird-FinC/financial_corrected_sql.json \
    --num_cpus $(nproc)

# BIRD-FinC (Data)
print_yellow "BIRD-FinC (Data)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_data.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_FinancialCorrected.json \
    --diff_json_path dataset/bird-FinC/financial_corrected.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_data.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_FinancialCorrected.json \
    --diff_json_path dataset/bird-FinC/financial_corrected.json \
    --num_cpus $(nproc)
