import fs from "node:fs";
import path from "node:path";

export interface SparkRuntime {
  repoRoot: string;
  sparkHome: string;
  sparkSubmit: string;
  sparkClass: string;
  projectJar: string;
}

interface ResolveSparkRuntimeInput {
  repoRoot: string;
  sparkHome?: string;
}

export function resolveSparkRuntime(input: ResolveSparkRuntimeInput): SparkRuntime {
  const sparkHome = input.sparkHome ?? process.env.SPARK_HOME;
  if (!sparkHome) {
    throw new Error(
      "SPARK_HOME must be set so the local web tool can start spark-submit and spark-class."
    );
  }

  const sparkSubmit = path.join(sparkHome, "bin", "spark-submit");
  const sparkClass = path.join(sparkHome, "bin", "spark-class");
  const projectJar = path.join(input.repoRoot, "target", "spark-uiservice-1.0-SNAPSHOT.jar");

  if (!fs.existsSync(sparkSubmit) || !fs.existsSync(sparkClass)) {
    throw new Error(`Spark binaries not found under ${path.join(sparkHome, "bin")}.`);
  }

  if (!fs.existsSync(projectJar)) {
    throw new Error(`Built project jar not found at ${projectJar}.`);
  }

  return {
    repoRoot: input.repoRoot,
    sparkHome,
    sparkSubmit,
    sparkClass,
    projectJar
  };
}
