#!/usr/bin/env python3
"""
ChartMuseum Benchmark Runner

Usage:
    python3 run_benchmark.py --model haiku --tools "" --title haiku_blind --subset 10 --parallelism 5
    python3 run_benchmark.py --model sonnet --tools "Read" --title sonnet_read --parallelism 5
"""
import argparse
import json
import subprocess
import time
import random
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data/chartmuseum/visual_dev.json")
EXPERIMENTS_DIR = os.path.join(BASE_DIR, "experiments")
BENCHMARK_FILE = os.path.join(BASE_DIR, "benchmark.txt")

DEFAULT_PROMPT = """Look at the chart image at {img_path}.

Question: {question}

Provide your answer concisely. Use this format:

<think>
... your reasoning ...
</think>
<answer>
... your final answer ...
</answer>"""

JUDGE_PROMPT = """You are provided with a question and two answers. Determine if they are equivalent.

Guidelines:
- Numbers: small relative differences are OK (32.35 ~ 32.34), but not large ones (32.35 != 35.25)
- Years/dates: exact match required
- Units: ignore if only one answer has them ($80 = 80)
- Text: ignore capitalization
- 0.6 = 60%

Question: {question}
Answer 1: {pred}
Answer 2: {gold}

Respond with only "Yes" or "No"."""


def extract_answer(text):
    """Extract answer from <answer> tags, or return last line."""
    match = re.search(r'<answer>\s*(.*?)\s*</answer>', text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    return lines[-1] if lines else text


def parse_stream_json(raw_output):
    """Parse stream-json output into structured session dict."""
    session = {
        "system": None,
        "events": [],
        "result": None,
    }
    for line in raw_output.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        etype = event.get("type", "")
        if etype == "system":
            session["system"] = event
        elif etype == "result":
            session["result"] = event
        else:
            session["events"].append(event)

    result_text = ""
    if session["result"]:
        result_text = session["result"].get("result", "")
    return result_text, session


def run_one(idx, example, model, tools, exp_id, answer_prompt, effort):
    """Run a single question and return result dict."""
    img_path = os.path.join(BASE_DIR, "data/chartmuseum", example['image'])
    question = example['question']
    gold = example['answer']
    session_id = f"q{idx}"
    sessions_dir = os.path.join(EXPERIMENTS_DIR, exp_id, "sessions")
    os.makedirs(sessions_dir, exist_ok=True)
    session_file = os.path.join(sessions_dir, f"{session_id}.json")

    prompt = answer_prompt.format(img_path=img_path, question=question)

    # Build command (stream-json captures full multi-turn conversation)
    # -- separates flags from positional prompt (needed because --tools is variadic)
    cmd = ["claude", "-p", "--verbose", "--model", model,
           "--output-format", "stream-json", "--no-session-persistence"]
    if tools == "":
        cmd.extend(["--tools", ""])
    else:
        cmd.extend(["--allowedTools", tools])
    if effort:
        cmd.extend(["--effort", effort])
    cmd.extend(["--", prompt])

    # Run answerer
    t0 = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        raw_output = result.stdout
    except subprocess.TimeoutExpired:
        raw_output = json.dumps({"type": "result", "result": "TIMEOUT", "is_error": True})
    except Exception as e:
        raw_output = json.dumps({"type": "result", "result": f"ERROR: {e}", "is_error": True})
    answer_time = time.time() - t0

    # Parse answer from stream
    answer_text, session = parse_stream_json(raw_output)
    pred = extract_answer(answer_text)

    # Save structured session as JSON
    with open(session_file, 'w') as f:
        json.dump(session, f, indent=2)

    # Run judge (haiku, no tools, plain text output)
    judge_prompt = JUDGE_PROMPT.format(question=question, pred=pred, gold=gold)
    judge_cmd = ["claude", "-p", "--model", "haiku", "--tools", "",
                 "--no-session-persistence", "--", judge_prompt]

    t0 = time.time()
    try:
        judge_result = subprocess.run(judge_cmd, capture_output=True, text=True, timeout=60)
        judge_raw = judge_result.stdout.strip().lower()
        is_correct = "yes" in judge_raw and "no" not in judge_raw.split("yes")[0]
    except Exception:
        is_correct = pred.strip().lower() == gold.strip().lower()
    judge_time = time.time() - t0

    num_turns = session["result"].get("num_turns", 1) if session["result"] else 1

    status = "Y" if is_correct else "N"
    print(f"  [q{idx:02d}] {status} | Gold: {gold[:30]:<30} | Pred: {pred[:30]:<30} | {answer_time:.0f}s | turns={num_turns}")

    return {
        "index": idx,
        "session_id": session_id,
        "session_file": f"sessions/{session_id}.json",
        "image": example['image'],
        "question": question,
        "gold": gold,
        "prediction": pred,
        "correct": is_correct,
        "num_turns": num_turns,
        "answer_time": round(answer_time, 1),
        "judge_time": round(judge_time, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="ChartMuseum Benchmark Runner")
    parser.add_argument("--model", default="haiku", help="Model for answering")
    parser.add_argument("--tools", default="", help="Tools to allow (empty = none, 'Read' = read only)")
    parser.add_argument("--title", required=True, help="Experiment title (used in ID)")
    parser.add_argument("--subset", type=int, default=0, help="Random subset size (0 = all 83)")
    parser.add_argument("--parallelism", type=int, default=5, help="Max parallel sessions")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for subset selection")
    parser.add_argument("--prompt", default=None, help="Custom answer prompt (use {img_path} and {question} placeholders)")
    parser.add_argument("--effort", default=None, choices=["low", "medium", "high", "max"], help="Effort/thinking level")
    args = parser.parse_args()

    answer_prompt = args.prompt if args.prompt else DEFAULT_PROMPT

    with open(DATA_PATH) as f:
        data = json.load(f)

    # Select subset
    if args.subset > 0:
        random.seed(args.seed)
        indices = sorted(random.sample(range(len(data)), min(args.subset, len(data))))
    else:
        indices = list(range(len(data)))
    subset = [(i, data[i]) for i in indices]

    # Experiment ID
    exp_id = f"{datetime.now().strftime('%Y%m%d_%H%M')}_{args.title}"
    tools_label = args.tools if args.tools else "none"

    effort_label = args.effort or "default"
    exp_dir = os.path.join(EXPERIMENTS_DIR, exp_id)
    os.makedirs(os.path.join(exp_dir, "sessions"), exist_ok=True)

    # Copy this script into the experiment folder for reproducibility
    shutil.copy2(__file__, os.path.join(exp_dir, "run_benchmark.py"))

    print(f"=== {exp_id} ===")
    print(f"    Model: {args.model} | Tools: {tools_label} | Effort: {effort_label} | Questions: {len(subset)}/{len(data)} | Parallelism: {args.parallelism}")
    print()

    # Run
    results = []
    with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = {
            pool.submit(run_one, i, ex, args.model, args.tools, exp_id, answer_prompt, args.effort): i
            for i, ex in subset
        }
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda x: x['index'])

    # Score
    correct = sum(r['correct'] for r in results)
    total = len(results)
    pct = 100 * correct / total if total > 0 else 0

    print(f"\n{'='*60}")
    print(f"  SCORE: {correct}/{total} ({pct:.1f}%)")
    print(f"{'='*60}")

    # Save results
    output = {
        "experiment_id": exp_id,
        "model": args.model,
        "tools": tools_label,
        "effort": args.effort or "default",
        "prompt": answer_prompt,
        "subset_indices": indices,
        "seed": args.seed,
        "score": correct,
        "total": total,
        "accuracy": round(correct / total, 4) if total > 0 else 0,
        "results": results,
    }
    results_path = os.path.join(exp_dir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(output, f, indent=2)

    # Append to benchmark.txt
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M')} | {exp_id} | model={args.model} | tools={tools_label} | effort={effort_label} | {correct}/{total} ({pct:.1f}%) | n={len(subset)} seed={args.seed}"
    with open(BENCHMARK_FILE, 'a') as f:
        f.write(line + '\n')

    print(f"  Experiment: experiments/{exp_id}/")
    print(f"  Logged to: benchmark.txt")


if __name__ == "__main__":
    main()
