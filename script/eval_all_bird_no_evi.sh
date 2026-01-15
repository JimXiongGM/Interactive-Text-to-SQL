print_yellow() {
  local text=$1
  echo -e "\033[1;33m${text}\033[0m"
}
print_yellow "Result of Ours. no_evidence"


print_yellow "BIRD-dev"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/dev.sql \
    --predicted_sql_path result/bird/predict/no_evidence/dev.json \
    --diff_json_path dataset/bird/dev.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/dev.sql \
    --predicted_sql_path result/bird/predict/no_evidence/dev.json \
    --diff_json_path dataset/bird/dev.json \
    --num_cpus $(nproc)

# BIRD-FinC (original)
print_yellow "BIRD-FinC (original)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_original.sql \
    --predicted_sql_path result/bird/predict/no_evidence/financial_corrected_original.json \
    --diff_json_path dataset/bird-FinC/financial.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_original.sql \
    --predicted_sql_path result/bird/predict/no_evidence/financial_corrected_original.json \
    --diff_json_path dataset/bird-FinC/financial.json \
    --num_cpus $(nproc)

# BIRD-FinC (SQL)
print_yellow "BIRD-FinC (SQL)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_sql.sql \
    --predicted_sql_path result/bird/predict/no_evidence/financial_corrected_sql.json \
    --diff_json_path dataset/bird-FinC/financial_corrected_sql.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_sql.sql \
    --predicted_sql_path result/bird/predict/no_evidence/financial_corrected_sql.json \
    --diff_json_path dataset/bird-FinC/financial_corrected_sql.json \
    --num_cpus $(nproc)

# BIRD-FinC (Data)
print_yellow "BIRD-FinC (Data)"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_data.sql \
    --predicted_sql_path result/bird/predict/no_evidence/financial_corrected_data.json \
    --diff_json_path dataset/bird-FinC/financial_corrected.json \
    --num_cpus $(nproc)
echo "VES"
python evaluation/bird_evaluation_ves_raw.py \
    --db_root_path dataset/bird/dev_databases/ \
    --ground_truth_path result/bird/golden/financial_corrected_data.sql \
    --predicted_sql_path result/bird/predict/no_evidence/financial_corrected_data.json \
    --diff_json_path dataset/bird-FinC/financial_corrected.json \
    --num_cpus $(nproc)

# BIRD-Minidev
print_yellow "BIRD-Minidev"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/predict/no_evidence/minidev_sqlite.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
echo "R-VES"
python -u evaluation/bird_minidev_evaluation_ves_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/predict/no_evidence/minidev_sqlite.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
echo "Soft F1"
python -u evaluation/bird_minidev_evaluation_f1_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/predict/no_evidence/minidev_sqlite.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
