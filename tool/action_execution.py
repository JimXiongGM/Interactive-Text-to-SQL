import os
from collections import Counter
from typing import List

from loguru import logger
from openai import BadRequestError

from tool.openai_api import chatgpt
from tool.utils import INVALID_RESULTS
from utils import colorful, read_json, save_to_json, timeout


def parse_action(text: str, execute=False, _actions=[]):
    """
    Note: The cleaned text result here is only for successful execution and will not enter the next round of interaction.
    """
    try:
        text = text.strip()

        # if "Action:" not in text:
        #     return "Error: No valid action found. Please provide an action after `Action:` ."
        action_str = text.split("Action:")
        if len(action_str) < 2:
            return "Error: No valid action found. Please provide an action after `Action:` ."
        if len(action_str) > 2:
            return (
                "Error: More than one `Action:` found. Please provide only one action."
            )

        # find the start index of SearchNodes, SearchGraphPatterns, ExecuteSPARQL, Done
        action_str = action_str[1]
        if not any(
            [
                i in action_str
                for i in [
                    "SearchColumn",
                    "SearchValue",
                    "FindShortestPath",
                    "ExecuteSQL",
                    "Done",
                ]
            ]
        ):
            return "Error: Action no found, action must be one of [SearchNodes, SearchGraphPatterns, ExecuteSPARQL, Done], one at a time."
        _s = [
            action_str.find(i)
            for i in [
                "SearchColumn",
                "SearchValue",
                "FindShortestPath",
                "ExecuteSQL",
                "Done",
            ]
        ]
        _s = min([i for i in _s if i != -1])
        action_str = action_str[_s:].strip()
        if action_str.startswith("Done"):
            return "Done"

        # clean \n after ExecuteSQL
        if "ExecuteSQL" in action_str:
            _idx = action_str.find("ExecuteSQL")
            action_str = (
                action_str[:_idx] + action_str[_idx:].replace("\n", " ").strip()
            )

        # find the last ) as the end.
        action_str = action_str[: action_str.rfind(")") + 1]

        if execute:
            SearchColumn, SearchValue, FindShortestPath, ExecuteSQL = _actions
            if "ExecuteSQL" in action_str:
                x = 1
            # may time out, return None
            obs = eval(action_str)
            return obs
        else:
            return action_str
    except Exception as e:
        # print_exc()
        err = f"Error: Action parsing error. {e.__class__.__name__}: {str(e)}"
        logger.error(f"{err}. action_str: {action_str}. Raw: {text}")
        return err


def _is_valid_action(action_str):
    if (
        not action_str
        or action_str.startswith("Error")
        or action_str.startswith("Done")
    ):
        return False
    return True


def is_valid_result(obs: str):
    if (
        str(obs) in INVALID_RESULTS
        or "Error" in str(obs)
        or " not supported" in str(obs)
    ):
        return False
    return True


def self_consistency_for_action(choices):
    """
    return: ranked (content, action)
    """
    # contents = [r["message"]["content"].strip() for r in response["choices"]]
    actions = [parse_action(i, execute=False) for i in choices]
    valid_actions = [i for i in actions if _is_valid_action(i)]
    actions_counter = Counter(valid_actions).most_common()

    # debug
    # if len(actions_counter) > 1:
    #     logger.info(f"actions_counter: {actions_counter}")

    # debug
    # logger.debug(f"contents:")
    # print("\n".join(contents))
    # logger.debug(f"actions_counter:")
    # print("\n".join([str(i) for i in actions_counter]))

    # rank actions by actions_counter
    ranked_choicess = []
    for action, _count in actions_counter:
        for choice in choices:
            if action in choice:
                ranked_choicess.append(choice)
                break
    return ranked_choicess, dict(actions_counter)


def preprocess_output(content: str):
    """
    Process the raw output directly based on rules.
    """

    if "\nAction: " in content:
        _cs = content.split("\nAction: ")
        content = "\nAction: ".join(_cs[:2])

    # Do not allow any newlines except \nAction
    content = content.replace("\n", " ")
    content = " ".join(content.split())
    content = content.replace("Action:", "\nAction:")
    return content.replace("[END]", "")


def _has_execution(messages: List[dict]):
    for m in messages[2:][::-1]:
        if m["role"] == "assistant" and "ExecuteSQL(" in m["content"]:
            return True
    return False


def retry_no_empty(func):
    def wrapper(*args, **kwargs):
        max_retries = 3
        for current_num in range(max_retries):
            res_json = func(*args, **kwargs)
            if res_json is None:
                return 0
            result_json = read_json(res_json)
            dialog = result_json["dialog"]
            for dia in dialog[:-1][::-1]:
                if dia["role"] == "user":
                    last_obs = dia["content"].replace("Observation: ", "").strip()
                    break
            if is_valid_result(last_obs):
                return 1
            elif current_num < max_retries - 1:
                os.remove(res_json)
        return 1  # Return the last result after max retries

    return wrapper


@retry_no_empty
@timeout(60 * 5)
def chat_with_LLM(
    d: dict,
    model_name: str,
    dataset: str,
    save_dir: str = None,
    tooldesc_demos: str = None,
    max_round_num: int = 8,
    add_evidence=False,
):
    """
    different between apis:
    same:
        - messages
        - temperature
        - top_p
        - stop
    chatgpt:
        - max_tokens
        - n
        - presence_penalty
        - frequency_penalty
    llama:
        "max_new_tokens": 512,
        "do_sample": True,
        "repetition_penalty": 1,
        "num_return_sequences": 1,
    """
    assert "id" in d, "id must be provided."
    assert "question" in d, "question must be provided."
    assert save_dir is not None, "save_dir must be provided."

    db = d["db_id"]
    print(f"id: {d['id']}")

    # load db-specific schema
    p = f"database/dbs_info/{dataset}/{db}.md"
    with open(p) as f:
        db_schema = f.read().strip()
    db_schema = f"Schema of database {db}:\n{db_schema}"

    question = d["question"].strip()

    # for inference model.
    tooldesc_demos = tooldesc_demos.strip()
    system = tooldesc_demos + f"\n\nNow, solve the following question step by step."

    start_text = f"{db_schema}\n\nQ: {question}"
    if add_evidence:
        evidence = d["evidence"].replace("\n", " ").strip()
        start_text += f"\nEvidence: {evidence}"

    # print(start_text)

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": start_text},
    ]

    round_idx = 0
    _last_out = ""

    # info
    completion_tokens = []
    prompt_tokens = []

    if dataset == "spider2-lite-sqlite":
        from tool import init_actions_spider2_sqlite as _init_actions
    elif dataset == "spider2-lite-snowflake":
        from tool import init_actions_spider2_snowflake as _init_actions
    elif "spider" in dataset:
        from tool import init_actions_spider as _init_actions
    elif "bird" in dataset:
        from tool import init_actions_bird as _init_actions
    else:
        raise ValueError(f"dataset: {dataset} is not supported.")

    _actions = [None, None, None, None]
    SearchColumn, SearchValue, FindShortestPath, ExecuteSQL = _init_actions(db=db)
    _actions[0] = SearchColumn
    _actions[1] = SearchValue
    _actions[2] = FindShortestPath
    _actions[3] = ExecuteSQL

    # history: all inp and out
    history = []

    while round_idx < max_round_num:
        if round_idx > 0:
            logger.debug(f"round_idx: {round_idx}")

        try:
            response = chatgpt(
                model=model_name,
                # db=db,
                messages=messages,
                stop=["\nObservation", "\nThought", "[END]"],
                temperature=0.7,
                max_tokens=512,
                n=1,
            )
        except BadRequestError as e:
            logger.error(f"BadRequestError: {e}")
            return
        if response is None:
            logger.error(f"response is None. id: {d['id']}")
            return
        if "usage" not in response:
            logger.error(response["error"])
            return

        prompt_tokens.append(response["usage"]["prompt_tokens"])
        completion_tokens.append(response["usage"]["completion_tokens"])

        # Preprocessing
        choices = [r["message"]["content"].strip() for r in response["choices"]]
        choices = [c.split("Observation")[0] for c in choices if c]

        # Process raw results directly
        choices = [preprocess_output(c) for c in choices]

        # debug
        # for c_i, c in enumerate(choices):
        #     print(colorful(f"choices[{c_i}]:"))
        #     print(c)

        # add self-consistency
        ranked_choicess, actions_counter = self_consistency_for_action(choices)

        history.append({"round_idx": round_idx, "choices": choices})

        # Try to execute the first valid action in actions as default observation
        out_thought_action = choices[0].strip()
        Observation = parse_action(out_thought_action, execute=True, _actions=_actions)

        # time out
        if Observation is None:
            return

        # If there is a valid observation, use this observation
        for content in ranked_choicess:
            _obs = parse_action(content, execute=True, _actions=_actions)
            if is_valid_result(_obs):
                Observation = _obs
                # Note: the content here is the raw model output, not cleaned
                out_thought_action = content.strip()
                break
        Observation = str(Observation).strip()

        # set flag to OFF for execute_sql tool.
        # treat the n output as ONE call.
        # if "(Hint: DOUBLE-CHECK the requirements" in Observation:
        #     _actions[3].set_flag1()
        if "(Hint: DOUBLE-CHECK the columns" in Observation:
            _actions[3].set_flag2()

        if out_thought_action == _last_out:
            messages.append({"role": "user", "content": "STOP because of repetition."})
            break
        _last_out = out_thought_action

        if not out_thought_action.startswith("Thought: "):
            out_thought_action = "Thought: " + out_thought_action
        messages.append({"role": "assistant", "content": out_thought_action})

        # debug
        print()
        print(colorful("LLM: ", color="yellow"), end="")
        print(out_thought_action.replace("\n", "\\n"))
        print(colorful("Observation: ", color="yellow"), end="")
        print(Observation.replace("\n", "\\n"))

        # if _debug:
        #     logger.debug(f"out_thought_action: {out_thought_action}")
        #     logger.debug(f"Observation: {Observation}")

        if Observation != "Done":
            messages.append(
                {
                    "role": "user",
                    "content": "Observation: " + Observation,  # + "\nThought: ",
                }
            )
            round_idx += 1
        else:
            # if Done but no Execution, continue
            if _has_execution(messages):
                messages.append({"role": "user", "content": "Stop condition detected."})
                break
            else:
                messages.append(
                    {
                        "role": "user",
                        "content": "Observation: Error. You must call ExecuteSQL function to provide the answer at least once.",
                    }
                )
                round_idx += 1

        # debug
        # break

    d["dialog"] = messages

    # add model info
    d["model_name"] = model_name
    d["completion_tokens"] = completion_tokens
    d["prompt_tokens"] = prompt_tokens

    save_to_json(d, f"{save_dir}/{d['id']}.json")
    return f"{save_dir}/{d['id']}.json"


if __name__ == "__main__":
    x = """I want search a column in the table 'battle_death' with the query 'injuries'.
Action: SearchColumn("injuries", topk=5)"""
    dataset = "spider"
    # print(
    #     parse_action(x, dataset=dataset, db="battle_death", execute=True, _actions=[None, None, None, None])
    # )
