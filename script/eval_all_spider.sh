print_yellow() {
  local text=$1
  echo -e "\033[1;33m${text}\033[0m"
}

# spider-dev
echo -e "\033[1;33mspider-dev\033[0m"
echo "EM, EX"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/dev.sql \
    --pred result/spider/predict/dev.txt \
    --db dataset/spider/database \
    --table dataset/spider/tables.json \
    --etype all
echo "TS"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/dev.sql \
    --pred result/spider/predict/dev.txt \
    --db evaluation/test_suite_sql_eval/testsuite_database \
    --table evaluation/test_suite_sql_eval/testsuite_tables.json \
    --etype exec
echo -e "\n"

# spider-test
echo -e "\033[1;33mspider-test\033[0m"
echo "EM, EX"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/test.sql \
    --pred result/spider/predict/test.txt \
    --db dataset/spider/test_database \
    --table dataset/spider/test_data/tables.json \
    --etype all
echo -e "\n"

# spider-dk
echo -e "\033[1;33mspider-dk\033[0m"
echo "EM, EX"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/dk.sql \
    --pred result/spider/predict/dk.txt \
    --db dataset/spider/database \
    --table dataset/Spider-DK/tables-Spider-DK.json \
    --etype all

# spider-syn
echo -e "\033[1;33mspider-syn\033[0m"
echo "EM, EX"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/syn.sql \
    --pred result/spider/predict/syn.txt \
    --db dataset/spider/database \
    --table dataset/spider/tables.json \
    --etype all
echo "TS"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/syn.sql \
    --pred result/spider/predict/syn.txt \
    --db evaluation/test_suite_sql_eval/testsuite_database \
    --table evaluation/test_suite_sql_eval/testsuite_tables.json \
    --etype exec
echo -e "\n"

# spider-realistic
echo -e "\033[1;33mspider-realistic\033[0m"
echo "EM, EX"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/realistic.sql \
    --pred result/spider/predict/realistic.txt \
    --db dataset/spider/database \
    --table dataset/spider/tables.json \
    --etype all
echo "TS"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/realistic.sql \
    --pred result/spider/predict/realistic.txt \
    --db evaluation/test_suite_sql_eval/testsuite_database \
    --table evaluation/test_suite_sql_eval/testsuite_tables.json \
    --etype exec
echo -e "\n"

# baseline
# TA-SQL
echo -e "\033[1;33mTA-SQL\033[0m"
echo "EM, EX"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/dev.sql \
    --pred result/spider/TA-SQL/spider_predict_dev.txt \
    --db dataset/spider/database \
    --table dataset/spider/tables.json \
    --etype all
echo "TS"
python evaluation/spider_eval_ts_raw.py \
    --gold result/spider/golden/dev.sql \
    --pred result/spider/TA-SQL/spider_predict_dev.txt \
    --db evaluation/test_suite_sql_eval/testsuite_database \
    --table evaluation/test_suite_sql_eval/testsuite_tables.json \
    --etype exec
echo -e "\n"