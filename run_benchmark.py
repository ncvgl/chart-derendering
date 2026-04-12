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

DEFAULT_PROMPT = """You are a chart analysis agent. Your goal is to answer a question about a chart image, but you must NOT answer from visual impression alone. Instead, follow this process:

STEP 1 — ANALYZE: Read the chart image at {img_path}. Understand what type of chart it is, what the axes and legend represent. Crop regions if needed for detail.

STEP 2 — PLAN: Based on the question, plan a computational method to find the answer. For example: if the question asks about crossings, plan to trace line positions and detect sign changes. If it asks about rankings, plan to measure bar heights. Write your plan before executing.

STEP 3 — EXECUTE: Use Bash to write Python scripts that analyze the image programmatically. Measure pixel values, trace lines, count elements, etc. Annotate the image with your findings (draw markers, labels, reference lines) and save it. Then Read the annotated image to verify your computation visually.

STEP 4 — ANSWER: Provide your answer grounded in the computation and annotations, not from visual impression.

You have access to Read and Bash tools. Use python3 with PIL/numpy for image analysis.

Question: {question}

<answer>
... your final answer ...
</answer>"""

DEVILS_ADVOCATE_PROMPT = """Look at the chart image at {img_path}.

Question: {question}

A colleague answered "{first_answer}" but you think they may be wrong.
Look very carefully at the chart and consider alternative answers.
Check if the colleague might have misread the chart, confused similar concepts, or looked at the wrong visual element.

What is the CORRECT answer?
<think>
... your reasoning about why the colleague might be wrong ...
</think>
<answer>
... your final answer (may be the same if you agree) ...
</answer>"""

CRITIC_PROMPT = """A model was asked to answer a question about a chart image. Review its reasoning and answer critically.

Question: {question}

Model's response:
{response}

Be a skeptical reviewer. Consider:
- Are there alternative interpretations of the question the model may have missed?
- Did the model confuse similar concepts (e.g. average vs range, total vs percentage)?
- Could the model be looking at the wrong part of the chart or misidentifying visual elements?
- Is the reasoning logically sound, or does it make unjustified leaps?

Point out specific weaknesses and suggest what the model should re-examine. Be concise and direct."""

REVISE_PROMPT = """Look at the chart image at {img_path}.

Question: {question}

You previously answered this question. Here was your response:
{response}

A reviewer found these potential issues with your analysis:
{critique}

Re-examine the chart with the reviewer's feedback in mind. Use Bash to write Python scripts for pixel-level analysis if needed. Annotate the image with your findings and Read it to verify.

Provide your revised (or confirmed) answer:

<think>
... your reasoning, addressing the reviewer's points ...
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
    """Extract answer from last <answer> tag (model may self-correct), or return last line."""
    matches = re.findall(r'<answer>\s*(.*?)\s*</answer>', text, re.DOTALL | re.IGNORECASE)
    if matches:
        return matches[-1].strip()
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


MAX_STALL_RETRIES = 0

def is_stall_timeout(raw_output):
    """Check if a timeout was a stall (0 events, never started) vs legitimate."""
    _, session = parse_stream_json(raw_output)
    return len(session["events"]) == 0 and session["system"] is None


FIRST_RESPONSE_TIMEOUT = 60   # seconds to wait for first model response after startup
IDLE_TIMEOUT = 180            # seconds of no stdout before declaring mid-session hang

def run_claude(cmd, timeout=300):
    """Run a claude -p command with timeout. Uses a reader thread to avoid blocking."""
    import signal, threading
    total_time = 0
    for attempt in range(1 + MAX_STALL_RETRIES):
        t0 = time.time()
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                    start_new_session=True)

            # Reader thread collects stdout and tracks activity
            output_lines = []
            got_assistant = threading.Event()
            last_activity = [time.time()]  # mutable so thread can update it

            def reader():
                for line in proc.stdout:
                    output_lines.append(line)
                    last_activity[0] = time.time()
                    if '"type":"assistant"' in line or '"type": "assistant"' in line:
                        got_assistant.set()

            def stderr_drain():
                for _ in proc.stderr:
                    pass

            t = threading.Thread(target=reader, daemon=True)
            t_err = threading.Thread(target=stderr_drain, daemon=True)
            t.start()
            t_err.start()

            def _kill_and_collect(reason):
                """Kill process, collect partial output, append timeout marker."""
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    proc.kill()
                proc.wait()
                t.join(timeout=5)
                # Keep partial output + append timeout result
                partial = ''.join(output_lines)
                timeout_line = json.dumps({"type": "result", "result": f"TIMEOUT ({reason})", "is_error": True})
                return partial + '\n' + timeout_line if partial else timeout_line

            # Wait for first assistant response (fast hang detection)
            if not got_assistant.wait(timeout=FIRST_RESPONSE_TIMEOUT):
                raw_output = _kill_and_collect("no first response in 60s")
            else:
                # Model started responding — poll for activity or completion
                while True:
                    t.join(timeout=10)  # check every 10s
                    if not t.is_alive():
                        # Process completed normally
                        proc.wait()
                        raw_output = ''.join(output_lines)
                        break
                    idle = time.time() - last_activity[0]
                    elapsed = time.time() - t0
                    if idle > IDLE_TIMEOUT:
                        raw_output = _kill_and_collect(f"no output for {IDLE_TIMEOUT}s")
                        break
                    if elapsed > timeout:
                        raw_output = _kill_and_collect(f"exceeded {timeout}s total")
                        break
        except Exception as e:
            raw_output = json.dumps({"type": "result", "result": f"ERROR: {e}", "is_error": True})
        elapsed = time.time() - t0
        total_time += elapsed

        # Only retry if it was a stall (never started), not a real timeout
        if is_stall_timeout(raw_output) and "TIMEOUT" in raw_output and attempt < MAX_STALL_RETRIES:
            wait = 10 * (attempt + 1)  # 10s, 20s, 30s...
            time.sleep(wait)
            continue
        break
    return raw_output, total_time


def run_one(idx, example, model, tools, exp_id, answer_prompt, effort, critic=False, devils_advocate=False, timeout=300):
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

    # Step 1: Run answerer
    raw_output, answer_time = run_claude(cmd, timeout=timeout)
    answer_text, session = parse_stream_json(raw_output)
    pred = extract_answer(answer_text)

    # Save step 1 session
    with open(session_file, 'w') as f:
        json.dump(session, f, indent=2)

    # Devil's advocate pass (if enabled and step 1 didn't timeout)
    if devils_advocate and pred and pred != "TIMEOUT":
        da_prompt = DEVILS_ADVOCATE_PROMPT.format(
            img_path=img_path, question=question, first_answer=pred[:100])
        da_cmd = ["claude", "-p", "--verbose", "--model", model,
                  "--output-format", "stream-json", "--no-session-persistence"]
        if tools == "":
            da_cmd.extend(["--tools", ""])
        else:
            da_cmd.extend(["--allowedTools", tools])
        if effort:
            da_cmd.extend(["--effort", effort])
        da_cmd.extend(["--", da_prompt])

        da_raw, da_time = run_claude(da_cmd, timeout=timeout)
        da_text, da_session = parse_stream_json(da_raw)
        answer_time += da_time

        with open(os.path.join(sessions_dir, f"{session_id}_da.json"), 'w') as f:
            json.dump(da_session, f, indent=2)

        if da_text and da_text != "TIMEOUT":
            answer_text = da_text
            pred = extract_answer(answer_text)

    # Steps 2-3: Critic loop (if enabled and step 1 didn't timeout)
    critique_text = ""
    if critic and answer_text and answer_text != "TIMEOUT":
        # Step 2: Opus critic (no tools, text only)
        critic_prompt = CRITIC_PROMPT.format(question=question, response=answer_text)
        critic_cmd = ["claude", "-p", "--model", "opus", "--tools", "",
                      "--effort", "high", "--no-session-persistence",
                      "--output-format", "stream-json", "--verbose",
                      "--", critic_prompt]
        critic_raw, critic_time = run_claude(critic_cmd, timeout=120)
        critique_text, critic_session = parse_stream_json(critic_raw)
        answer_time += critic_time

        # Save critic session
        with open(os.path.join(sessions_dir, f"{session_id}_critic.json"), 'w') as f:
            json.dump(critic_session, f, indent=2)

        if critique_text and critique_text != "TIMEOUT":
            # Step 3: Sonnet revises with critique
            revise_prompt = REVISE_PROMPT.format(
                img_path=img_path, question=question,
                response=answer_text, critique=critique_text)
            revise_cmd = ["claude", "-p", "--verbose", "--model", model,
                          "--output-format", "stream-json", "--no-session-persistence"]
            if tools == "":
                revise_cmd.extend(["--tools", ""])
            else:
                revise_cmd.extend(["--allowedTools", tools])
            if effort:
                revise_cmd.extend(["--effort", effort])
            revise_cmd.extend(["--", revise_prompt])

            revise_raw, revise_time = run_claude(revise_cmd, timeout=timeout)
            revise_text, revise_session = parse_stream_json(revise_raw)
            answer_time += revise_time

            # Save revise session
            with open(os.path.join(sessions_dir, f"{session_id}_revise.json"), 'w') as f:
                json.dump(revise_session, f, indent=2)

            if revise_text and revise_text != "TIMEOUT":
                answer_text = revise_text
                pred = extract_answer(answer_text)

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
    parser.add_argument("--model", default="sonnet", help="Model for answering")
    parser.add_argument("--tools", default="Read,Bash", help="Tools to allow (empty = none, 'Read' = read only, 'Read,Bash' = read + bash)")
    parser.add_argument("--title", required=True, help="Experiment title (used in ID)")
    parser.add_argument("--subset", type=int, default=0, help="Random subset size (0 = all 83)")
    parser.add_argument("--indices", default=None, help="Comma-separated question indices to run (e.g. '13' or '3,13,28')")
    parser.add_argument("--parallelism", type=int, default=5, help="Max parallel sessions")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for subset selection")
    parser.add_argument("--prompt", default=None, help="Custom answer prompt (use {img_path} and {question} placeholders)")
    parser.add_argument("--effort", default=None, choices=["low", "medium", "high", "max"], help="Effort/thinking level")
    parser.add_argument("--timeout", type=int, default=300, help="Timeout per claude -p call in seconds (default 300)")
    parser.add_argument("--critic", action="store_true", help="Enable Opus critic step between answer and revision")
    parser.add_argument("--devils-advocate", action="store_true", help="Two-pass: answer then challenge with devil's advocate")
    args = parser.parse_args()

    answer_prompt = args.prompt if args.prompt else DEFAULT_PROMPT

    with open(DATA_PATH) as f:
        data = json.load(f)

    # Select subset
    if args.indices:
        indices = sorted([int(i) for i in args.indices.split(',')])
    elif args.subset > 0:
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
    exp_start = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = {
            pool.submit(run_one, i, ex, args.model, args.tools, exp_id, answer_prompt, args.effort, args.critic, args.devils_advocate, args.timeout): i
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
        "critic": args.critic,
        "devils_advocate": args.devils_advocate,
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
    elapsed = time.time() - exp_start
    elapsed_str = f"{int(elapsed//60)}m{int(elapsed%60):02d}s"
    line = f"{correct}/{total} ({pct:.1f}%) | {datetime.now().strftime('%Y-%m-%d %H:%M')} | {exp_id} | model={args.model} | tools={tools_label} | effort={effort_label} | n={len(subset)} seed={args.seed} | {elapsed_str}"
    with open(BENCHMARK_FILE, 'a') as f:
        f.write(line + '\n')

    print(f"  Experiment: experiments/{exp_id}/")
    print(f"  Logged to: benchmark.txt")


if __name__ == "__main__":
    main()
