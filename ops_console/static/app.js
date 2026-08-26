(() => {
  const api = async (path, token) => {
    const headers = token ? { Authorization: `Bearer ${token}` } : {};
    const response = await fetch(path, { headers, cache: 'no-store' });
    if (!response.ok) {
      let detail = `${response.status} ${response.statusText}`;
      try { detail = (await response.json()).detail || detail; } catch (_) {}
      throw new Error(detail);
    }
    return response.json();
  };

  const renderFallback = () => {
    const root = document.getElementById('app');
    root.innerHTML = `
      <main class="shell">
        <header><div><p class="eyebrow">AI-BYBIT / PAPER ONLY</p><h1>实战运维台</h1></div>
        <button id="refresh">刷新</button></header>
        <section class="token-row"><input id="token" type="password" placeholder="OPS_CONSOLE_TOKEN" /></section>
        <pre id="fallback-output" class="raw">Vue CDN 不可用，已切换离线只读模式。</pre>
      </main>`;
    const token = document.getElementById('token');
    token.value = sessionStorage.getItem('opsToken') || '';
    const refresh = async () => {
      sessionStorage.setItem('opsToken', token.value);
      const output = document.getElementById('fallback-output');
      try { output.textContent = JSON.stringify(await api('/api/status', token.value), null, 2); }
      catch (error) { output.textContent = String(error); }
    };
    document.getElementById('refresh').addEventListener('click', refresh);
    refresh();
  };

  if (!window.Vue) {
    renderFallback();
    return;
  }

  const { createApp, computed, onMounted, onUnmounted, ref } = window.Vue;
  createApp({
    setup() {
      const token = ref(sessionStorage.getItem('opsToken') || '');
      const snapshot = ref(null);
      const events = ref([]);
      const error = ref('');
      const loading = ref(false);
      let timer = null;

      const overallClass = computed(() => `state-${snapshot.value?.overall || 'unknown'}`);
      const refresh = async () => {
        loading.value = true;
        error.value = '';
        sessionStorage.setItem('opsToken', token.value);
        try {
          const [status, history] = await Promise.all([
            api('/api/status', token.value), api('/api/events', token.value)
          ]);
          snapshot.value = status;
          events.value = history.items || [];
        } catch (err) {
          error.value = String(err.message || err);
        } finally {
          loading.value = false;
        }
      };
      const label = (value) => value === true ? '正常' : value === false ? '异常' : '未知';
      const json = (value) => JSON.stringify(value || {}, null, 2);
      onMounted(() => {
        refresh();
        timer = window.setInterval(refresh, 5000);
      });
      onUnmounted(() => window.clearInterval(timer));
      return { token, snapshot, events, error, loading, refresh, overallClass, label, json };
    },
    template: `
      <main class="shell">
        <header>
          <div><p class="eyebrow">AI-BYBIT / PRACTICAL PAPER</p><h1>两节点实战运维台</h1>
            <p class="subtitle">只读监控 · Shadow/Paper · 不提供主网开关</p></div>
          <button @click="refresh" :disabled="loading">{{ loading ? '读取中…' : '立即刷新' }}</button>
        </header>
        <section class="token-row">
          <label>运维访问令牌</label><input v-model="token" type="password" autocomplete="off" placeholder="OPS_CONSOLE_TOKEN" />
          <span>仅保存在当前浏览器会话</span>
        </section>
        <p v-if="error" class="error">{{ error }}</p>
        <section v-if="snapshot" class="hero" :class="overallClass">
          <div><p>综合状态</p><strong>{{ snapshot.overall.toUpperCase() }}</strong></div>
          <div><p>采样时间</p><span>{{ snapshot.as_of }}</span></div>
          <div><p>Paper-only</p><span>{{ label(snapshot.safety.paper_only) }}</span></div>
        </section>
        <section v-if="snapshot" class="grid">
          <article class="card"><div class="card-title"><h2>预测节点</h2><span :class="snapshot.predictor.ready ? 'ok' : 'bad'">{{ label(snapshot.predictor.ready) }}</span></div>
            <dl><dt>Live</dt><dd>{{ label(snapshot.predictor.live) }}</dd><dt>延迟</dt><dd>{{ snapshot.predictor.latency_ms }} ms</dd>
            <dt>Cluster</dt><dd>{{ snapshot.predictor.capabilities.cluster_id || '—' }}</dd><dt>Deployment</dt><dd>{{ snapshot.predictor.capabilities.deployment_id || '—' }}</dd></dl>
            <details><summary>完整健康信息</summary><pre>{{ json(snapshot.predictor) }}</pre></details></article>
          <article class="card"><div class="card-title"><h2>执行节点</h2><span :class="snapshot.executor.ready ? 'ok' : 'bad'">{{ label(snapshot.executor.ready) }}</span></div>
            <dl><dt>Live</dt><dd>{{ label(snapshot.executor.live) }}</dd><dt>模式</dt><dd>{{ snapshot.safety.executor_mode }}</dd>
            <dt>Kill switch</dt><dd>{{ label(snapshot.safety.kill_switch) }}</dd><dt>Incident</dt><dd>{{ snapshot.safety.incident_mode || '—' }}</dd></dl>
            <details><summary>完整健康信息</summary><pre>{{ json(snapshot.executor) }}</pre></details></article>
          <article class="card safety"><div class="card-title"><h2>安全约束</h2><span :class="snapshot.safety.paper_only ? 'ok' : 'bad'">{{ label(snapshot.safety.paper_only) }}</span></div>
            <dl><dt>Control mode</dt><dd>{{ snapshot.safety.control_execution_mode }}</dd><dt>Executor mode</dt><dd>{{ snapshot.safety.executor_execution_mode }}</dd>
            <dt>主网允许</dt><dd>否</dd><dt>Dead letter</dt><dd>{{ snapshot.safety.dead_letter_count ?? '—' }}</dd>
            <dt>未完成票据</dt><dd>{{ snapshot.safety.incomplete_ticket_count ?? '—' }}</dd></dl></article>
          <article class="card"><div class="card-title"><h2>状态变更</h2><span>{{ events.length }}</span></div>
            <ol class="events"><li v-for="event in events.slice(0,12)" :key="event.at + event.overall"><time>{{ event.at }}</time><b>{{ event.overall }}</b><span>P={{ event.predictor_ready }} / E={{ event.executor_ready }}</span></li></ol></article>
        </section>
        <footer>此页面不包含下单、解锁、修改数据库或主网操作。异常时先冻结新风险并按运行手册处置。</footer>
      </main>`
  }).mount('#app');
})();
