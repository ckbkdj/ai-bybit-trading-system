# syntax=docker/dockerfile:1.7
FROM python:3.11-slim-bookworm AS locked-runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/ai-bybit
COPY requirements/linux-py311.lock /opt/ai-bybit/requirements/linux-py311.lock
RUN python -m pip install --require-hashes -r /opt/ai-bybit/requirements/linux-py311.lock

FROM locked-runtime AS predictor
RUN groupadd --gid 10001 ai-bybit \
    && useradd --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin ai-bybit \
    && mkdir -p /var/lib/ai-bybit/predictor-realtime \
        /var/lib/ai-bybit/publication-worker \
        /var/lib/ai-bybit/control-plane \
        /var/lib/ai-bybit/market-collector \
    && chown -R 10001:10001 /var/lib/ai-bybit
COPY --chown=10001:10001 ai_bot3/ai_bot3 /opt/ai-bybit/ai_bot3/ai_bot3
COPY --chown=10001:10001 shadow_contracts /opt/ai-bybit/shadow_contracts
COPY --chown=10001:10001 scripts /opt/ai-bybit/scripts
COPY --chown=10001:10001 deploy/docker/tcp_healthcheck.py /opt/ai-bybit/tcp_healthcheck.py
ENV PYTHONPATH=/opt/ai-bybit:/opt/ai-bybit/ai_bot3/ai_bot3
WORKDIR /opt/ai-bybit/ai_bot3/ai_bot3
USER 10001:10001
ENTRYPOINT ["python"]
CMD ["api/control_plane_server.py"]

FROM locked-runtime AS executor
RUN groupadd --gid 10002 ai-bybit-executor \
    && useradd --uid 10002 --gid 10002 --home-dir /nonexistent --shell /usr/sbin/nologin ai-bybit-executor \
    && mkdir -p /var/lib/ai-bybit/executor \
    && chown -R 10002:10002 /var/lib/ai-bybit
COPY --chown=10002:10002 BybitContractBotV4 /opt/ai-bybit/BybitContractBotV4
COPY --chown=10002:10002 shadow_contracts /opt/ai-bybit/shadow_contracts
COPY --chown=10002:10002 deploy/docker/tcp_healthcheck.py /opt/ai-bybit/tcp_healthcheck.py
ENV PYTHONPATH=/opt/ai-bybit:/opt/ai-bybit/BybitContractBotV4
WORKDIR /opt/ai-bybit/BybitContractBotV4
USER 10002:10002
ENTRYPOINT ["python"]
CMD ["main.py"]

FROM locked-runtime AS ops-console
RUN groupadd --gid 10003 ai-bybit-ops \
    && useradd --uid 10003 --gid 10003 --home-dir /nonexistent --shell /usr/sbin/nologin ai-bybit-ops
COPY --chown=10003:10003 ops_console /opt/ai-bybit/ops_console
WORKDIR /opt/ai-bybit/ops_console
USER 10003:10003
EXPOSE 8790
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8790", "--no-access-log"]
