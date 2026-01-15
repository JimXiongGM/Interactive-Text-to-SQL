print_yellow() {
  local text=$1
  echo -e "\033[1;33m${text}\033[0m"
}

print_yellow "Baseline of BIRD-Minidev"

# zero-shot
print_yellow "Zero-shot no evidence"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_minidev.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
echo "R-VES"
python -u evaluation/bird_minidev_evaluation_ves_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_minidev.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
echo "Soft F1"
python -u evaluation/bird_minidev_evaluation_f1_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/zero_shot_minidev.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)


# DIN-SQL
print_yellow "DIN-SQL no evidence"
echo "EX"
python -u evaluation/bird_evaluation_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_minidev.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
echo "R-VES"
python -u evaluation/bird_minidev_evaluation_ves_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_minidev.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
echo "Soft F1"
python -u evaluation/bird_minidev_evaluation_f1_raw.py \
    --db_root_path dataset/bird-minidev/dev_databases/ \
    --ground_truth_path dataset/bird-minidev/mini_dev_sqlite_gold.sql \
    --predicted_sql_path result/bird/baseline/no_evidence_True/din_sql_minidev.json \
    --diff_json_path dataset/bird-minidev/mini_dev_sqlite.json \
    --num_cpus $(nproc)
