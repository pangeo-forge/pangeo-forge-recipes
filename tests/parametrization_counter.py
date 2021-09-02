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

command_list = ["pytest", "--disable-warnings", "--collect-only"]


def parametrization_counter(filename):
    cmd = command_list + ["-q", filename]
    output = subprocess.check_output(cmd)
    output = output.decode("utf-8")
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
    summary = summary.decode("utf-8")
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
            fig, ax = plt.subplots(1, 1)

            # aggregate values to plot
            flattened = {}
            for fname in details.keys():
                for test in details[fname]["detail"].keys():
                    flattened.update({test: details[fname]["detail"][test]})

            keys = flattened.keys()
            values = flattened.values()
            yticks = range(len(keys))
            plt.barh(yticks, values)
            plt.yticks(yticks, labels=keys)
            plt.xticks(range(0, max(values) + 1, 50))
            plt.tight_layout()
            plt.show()
