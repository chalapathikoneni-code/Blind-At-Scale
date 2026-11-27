#!/usr/bin/env python3
"""V5 AWS CLI construction checks (offline, no live AWS call).

Mirrors the synthetic package's V5:
  1. Flag parsing with the real AWS CLI v1 (nonexistent files -> the
     CLI must fail ONLY on the missing file, before any network call).
  2. botocore validate_parameters on the RunJobFlow and
     AddJobFlowSteps parameter shapes the scripts would produce.
  3. Shape-member audit: every key in fleet.json,
     instance_groups.json and the step JSON must be a member of its
     EMR service-model shape.
"""
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PKG = HERE.parent / "realdata_emr_package"
AWS = str(HERE / "awsvenv" / "bin" / "aws")
FAILED = []


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        FAILED.append(name)


def cli(args):
    return subprocess.run([AWS] + args, capture_output=True, text=True)


def main():
    # ---- 1. Flag parsing (real CLI, no network) ----
    p = cli(["emr", "create-cluster",
             "--name", "blind-at-scale-emr-realdata",
             "--release-label", "emr-7.14.0",
             "--applications", "Name=Spark",
             "--use-default-roles",
             "--region", "us-east-1",
             "--log-uri", "s3://dummy-bucket/blind-at-scale/emr-realdata/logs/",
             "--enable-debugging",
             "--instance-fleets", "file:///nonexistent/fleet.json",
             "--bootstrap-actions",
             "Path=s3://dummy-bucket/blind-at-scale/emr-realdata/code/bootstrap_action.sh,Name=BAS pinned numpy+pyarrow+xxhash",
             "--tags", "Name=blind-at-scale-emr-realdata",
             "Project=blind-at-scale",
             "--query", "ClusterId", "--output", "text"])
    check("create-cluster flags parse (fails only on missing fleet file)",
          p.returncode != 0
          and "nonexistent" in (p.stderr + p.stdout),
          (p.stderr + p.stdout).strip().splitlines()[-1][:160]
          if (p.stderr + p.stdout).strip() else "no output")

    p = cli(["emr", "add-steps", "--cluster-id", "j-XXXXXXXXXXXXX",
             "--region", "us-east-1",
             "--steps", "file:///nonexistent/step.json",
             "--query", "StepIds", "--output", "text"])
    check("add-steps flags parse (fails only on missing steps file)",
          p.returncode != 0
          and "nonexistent" in (p.stderr + p.stdout),
          (p.stderr + p.stdout).strip().splitlines()[-1][:160]
          if (p.stderr + p.stdout).strip() else "no output")

    p = cli(["emr", "create-cluster", "--bogus-flag-xyz"])
    check("bogus flag rejected (parser is live)",
          p.returncode != 0
          and "Unknown options" in (p.stderr + p.stdout), "")

    # ---- 2. Wire-shape validation (botocore) ----
    sys.path.insert(0, str(HERE / "awsvenv" / "lib" / "python3.12"
                           / "site-packages"))
    import botocore.session
    session = botocore.session.get_session()
    model = session.get_service_model("emr")

    fleet = json.loads((PKG / "config" / "fleet.json").read_text())
    ig = json.loads((PKG / "config" / "instance_groups.json").read_text())
    run_params = {
        "Name": "blind-at-scale-emr-realdata",
        "ReleaseLabel": "emr-7.14.0",
        "Applications": [{"Name": "Spark"}],
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "LogUri": "s3://dummy-bucket/blind-at-scale/emr-realdata/logs/",
        "Instances": {"InstanceFleets": fleet,
                      "KeepJobFlowAliveWhenNoSteps": True,
                      "TerminationProtected": False},
        "BootstrapActions": [{"Name": "BAS pinned numpy+pyarrow+xxhash",
                              "ScriptBootstrapAction": {
                                  "Path": "s3://dummy-bucket/blind-at-scale/emr-realdata/code/bootstrap_action.sh"}}],
        "Tags": [{"Key": "Name", "Value": "blind-at-scale-emr-realdata"},
                 {"Key": "Project", "Value": "blind-at-scale"}],
        "VisibleToAllUsers": True,
    }
    op = model.operation_model("RunJobFlow")
    report = op.input_shape.validate(run_params) if hasattr(
        op.input_shape, "validate") else None
    # botocore's validate_parameters is on the client, not the shape;
    # use the documented client-side validator via a dry client.
    from botocore.validate import validate_parameters
    try:
        validate_parameters(run_params, op.input_shape)
        check("RunJobFlow (fleet) passes botocore validate_parameters",
              True, "")
    except Exception as e:  # noqa: BLE001 - report the validation error
        check("RunJobFlow (fleet) passes botocore validate_parameters",
              False, str(e)[:300])

    run_params_ig = dict(run_params)
    run_params_ig["Instances"] = {"InstanceGroups": ig,
                                  "KeepJobFlowAliveWhenNoSteps": True,
                                  "TerminationProtected": False}
    try:
        validate_parameters(run_params_ig, op.input_shape)
        check("RunJobFlow (instance-groups fallback) passes validation",
              True, "")
    except Exception as e:  # noqa: BLE001
        check("RunJobFlow (instance-groups fallback) passes validation",
              False, str(e)[:300])

    # Step JSON: render the script's heredoc verbatim with dummy values.
    step_text = (PKG / "scripts" / "add_job_step.sh").read_text()
    heredoc = step_text.split('cat > "$STEP_FILE" <<EOF\n', 1)[1]
    heredoc = heredoc.split("\nEOF", 1)[0]
    heredoc = heredoc.replace("$BUCKET", "dummy-bucket")
    heredoc = heredoc.replace("$PREFIX",
                              "blind-at-scale/emr-realdata")
    steps = json.loads(heredoc)
    check("step JSON parses from the script heredoc", True,
          f"{len(steps)} step(s), {len(steps[0]['HadoopJarStep']['Args'])} args")
    add_op = model.operation_model("AddJobFlowSteps")
    try:
        validate_parameters({"JobFlowId": "j-XXXXXXXXXXXXX",
                             "Steps": steps}, add_op.input_shape)
        check("AddJobFlowSteps passes botocore validate_parameters",
              True, "")
    except Exception as e:  # noqa: BLE001
        check("AddJobFlowSteps passes botocore validate_parameters",
              False, str(e)[:300])

    # ---- 3. Shape-member audit ----
    def audit(obj, shape, path=""):
        bad = []
        if shape.type_name == "structure":
            for k, v in obj.items():
                if k not in shape.members:
                    bad.append(f"{path}{k} (not in {shape.name})")
                else:
                    bad += audit(v, shape.members[k], f"{path}{k}.")
        elif shape.type_name == "list":
            for i, v in enumerate(obj):
                bad += audit(v, shape.member, f"{path}[{i}].")
        return bad

    fleet_shape = model.shape_for("InstanceFleetConfig")
    bad = []
    for i, f in enumerate(fleet):
        bad += audit(f, fleet_shape, f"fleet[{i}].")
    check("fleet.json keys all in InstanceFleetConfig shape",
          not bad, "; ".join(bad)[:300] if bad else f"{len(fleet)} fleets")

    ig_shape = model.shape_for("InstanceGroupConfig")
    bad = []
    for i, g in enumerate(ig):
        bad += audit(g, ig_shape, f"ig[{i}].")
    check("instance_groups.json keys all in InstanceGroupConfig shape",
          not bad, "; ".join(bad)[:300] if bad else f"{len(ig)} groups")

    step_shape = model.shape_for("StepConfig")
    bad = []
    for i, s in enumerate(steps):
        bad += audit(s, step_shape, f"steps[{i}].")
    check("step JSON keys all in StepConfig shape",
          not bad, "; ".join(bad)[:300] if bad else "")

    # spark-submit arg sanity: the job path and --py-files must point
    # at the shipped code names.
    args = steps[0]["HadoopJarStep"]["Args"]
    check("step ships realdata_model.py via --py-files",
          "s3://dummy-bucket/blind-at-scale/emr-realdata/code/realdata_model.py"
          in args, "")
    check("step runs spark_job_realdata.py",
          "s3://dummy-bucket/blind-at-scale/emr-realdata/code/spark_job_realdata.py"
          in args, "")
    check("step pins shuffle partitions 128",
          "spark.sql.shuffle.partitions=128" in args, "")
    check("step disables adaptive execution",
          "spark.sql.adaptive.enabled=false" in args, "")
    check("step disables broadcast joins",
          "spark.sql.autoBroadcastJoinThreshold=-1" in args, "")
    check("step event log goes to the realdata prefix",
          "spark.eventLog.dir=s3a://dummy-bucket/blind-at-scale/emr-realdata/eventlog/"
          in args, "")

    print(f"\nV5 AWS CHECKS: "
          f"{'ALL CHECKS PASSED' if not FAILED else str(FAILED)}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
