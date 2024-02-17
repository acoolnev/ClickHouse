FROM public.ecr.aws/lambda/provided:al2023

ENV TZ UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY bootstrap ${LAMBDA_RUNTIME_DIR}
COPY clickhouse-lambda ${LAMBDA_TASK_ROOT}

# /lambda-entrypoint.sh expects a handler name as first parameter. It is not used by 
# clickhouse-lambda yet. Later it can be used to pass request payload schema to handle 
# e.g. AWS events.
CMD ["lambda"]
