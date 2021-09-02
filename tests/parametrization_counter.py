"""
Count how many parametrizations are called per module or per test.
Example usage:
    1. Summarize calls per module:
        `python parametrization_counter.py summary`
    2. Detail calls per specific test within each module:
        a. Text output to terminal:
            `python parametrization_counter.py details text`
        b. Plot output:
            `python parametrization_counter.py details plot`
"""
import subprocess
import sys

import matplotlib.pyplot as plt
from rich import print

command_list = ["pytest", "--disable-warnings", "--collect-only"]


def parametrization_counter(filename):
    cmd = command_list + ["-q", filename]
    output = subprocess.check_output(cmd)
    output = output.decode('utf-8')
    lines = output.split("\n")
    lines = [line for line in lines if "::" in line]
    lines = [line.split("::")[1] for line in lines]
    lines = [line.split("[")[0] for line in lines]
    uniques = list(set(lines))
    counts = {test: lines.count(test) for test in uniques}
    return counts


if __name__ == "__main__":
    args = sys.argv[1:]

    cmd = command_list + ["-qq"]
    summary = subprocess.check_output(cmd)
    summary = summary.decode('utf-8')
    summary = summary.split("\n")
    summary = [line for line in summary if "test" in line]

    if args[0] == "summary":
        print()
        for line in summary:
            print(line)
        print()
    elif args[0] == "details":
        filenames = [line.split("tests/")[1] for line in summary]
        filenames = [f.split(":")[0] for f in filenames]
        details = {}
        for f in filenames:
            print(f"Collecting counts for {f}...")
            summary_line = [line for line in summary if f in line]
            fname, total = summary_line[0].split(":")[0], summary_line[0].split(":")[1]
            counts = parametrization_counter(f)
            details.update({fname: {"total": int(total), "detail": counts}})
        if args[1] == "text":
            print(details)
        elif args[1] == "plot":
            nrows = (len(details.keys()) // 3) + 1
            fig, axs = plt.subplots(nrows=nrows, ncols=3, figsize=(20, 10))
            row = col = 0
            for i, k in enumerate(details.keys()):
                if i // 3 != row:
                    row += 1
                keys = details[k]["detail"].keys()
                values = details[k]["detail"].values()
                yticks = range(len(keys))
                axs[row, col].barh(yticks, values)
                axs[row, col].set_yticks(yticks)
                axs[row, col].set_yticklabels(keys)
                axs[row, col].set_xticks(range(max(values)+1))
                axs[row, col].set_title(label=f"{k}: {details[k]['total']} calls")
                axs[row, col].set_box_aspect(1)
                col += 1
                col = col if col < 3 else 0
                plt.tight_layout()
            for row in axs:
                for ax in row:
                    # hide unused axes
                    if not ax.patches:
                        ax.set_axis_off()
            plt.show()
