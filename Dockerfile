# Batch-step Lambda image for the Step Functions Task states (ingest, load, gates, dbt).
# CI builds it to prove it builds; only the gated deploy workflow pushes it to ECR.
FROM ghcr.io/astral-sh/uv:0.13.1 AS uv

FROM public.ecr.aws/lambda/python:3.12 AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /build
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
# Locked runtime dependencies + the Athena adapter, installed into the Lambda task root.
RUN uv export --frozen --no-dev --extra athena --no-emit-project -o requirements.txt \
 && uv pip install --no-cache --target "${LAMBDA_TASK_ROOT}" -r requirements.txt \
 && uv pip install --no-cache --no-deps --target "${LAMBDA_TASK_ROOT}" .

FROM public.ecr.aws/lambda/python:3.12
COPY --from=build ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}
COPY dbt ${LAMBDA_TASK_ROOT}/dbt
ENV UBER_DBT_DIR=${LAMBDA_TASK_ROOT}/dbt \
    DBT_PROFILES_DIR=${LAMBDA_TASK_ROOT}/dbt \
    DBT_LOG_PATH=/tmp/dbt-logs \
    DBT_TARGET_PATH=/tmp/dbt-target \
    UBER_LOG_FORMAT=json
CMD ["uber_rides.orchestration.steps.handler"]
