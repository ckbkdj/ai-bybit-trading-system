from __future__ import annotations

from core.evaluation.profitability_rebuild_components import *  # noqa: F401,F403

class ProfitabilityRebuild:
    def __init__(self, config: ProfitabilityRebuildConfig) -> None:
        self.config = config
        self.source = KlinePanelSource(
            config.feature_store_path,
            source=config.kline_source,
        )
        self.ledger = TrialLedger(config.trial_ledger_path)
        self.bybit_pit_snapshot_maximum_sequence = None
        self.bybit_pit_snapshot_maximum_invalidation_rowid = None
        self.bybit_pit_snapshot_maximum_capture_audit_rowid = None
        self.bybit_pit_snapshot_maximum_import_rowid = None
        if config.bybit_pit_store_path is not None:
            bybit_source = BybitPITFeatureSource(
                config.bybit_pit_store_path
            )
            (
                self.bybit_pit_snapshot_maximum_sequence,
                self.bybit_pit_snapshot_maximum_invalidation_rowid,
            ) = bybit_source.snapshot_watermarks()
            (
                self.bybit_pit_snapshot_maximum_capture_audit_rowid,
                self.bybit_pit_snapshot_maximum_import_rowid,
            ) = bybit_source.evidence_watermarks()
        self.macro_pit_snapshot_maximum_sequence = None
        if config.macro_pit_store_path is not None:
            self.macro_pit_snapshot_maximum_sequence = MacroPITFeatureSource(
                config.macro_pit_store_path,
                verify_raw_hashes=config.verify_macro_raw_hashes,
            ).maximum_sequence()
        self.flow_pit_snapshot_maximum_sequence = None
        self.flow_pit_snapshot_maximum_invalidation_rowid = None
        if config.flow_pit_store_path is not None:
            flow_source = FlowPITFeatureSource(
                config.flow_pit_store_path,
                verify_raw_hashes=config.verify_flow_raw_hashes,
            )
            (
                self.flow_pit_snapshot_maximum_sequence,
                self.flow_pit_snapshot_maximum_invalidation_rowid,
            ) = flow_source.snapshot_watermarks()
        self.feature_store_identity = _stable_file_identity(
            config.feature_store_path
        )
        self.feature_store_snapshot = (
            int(self.feature_store_identity["size_bytes"]),
            int(self.feature_store_identity["modified_ns"]),
        )
        run_payload = {
            "code_commit": config.code_commit,
            "feature_store": self.feature_store_identity,
            "kline_source": config.kline_source,
            "trad_panel_root": (
                str(config.trad_panel_root.resolve()) if config.trad_panel_root else None
            ),
            "verify_trad_panel_sha256": config.verify_trad_panel_sha256,
            "bybit_pit_store": (
                str(config.bybit_pit_store_path.resolve())
                if config.bybit_pit_store_path
                else None
            ),
            "bybit_pit_snapshot_maximum_sequence": self.bybit_pit_snapshot_maximum_sequence,
            "bybit_pit_snapshot_maximum_invalidation_rowid": (
                self.bybit_pit_snapshot_maximum_invalidation_rowid
            ),
            "bybit_pit_snapshot_maximum_capture_audit_rowid": (
                self.bybit_pit_snapshot_maximum_capture_audit_rowid
            ),
            "bybit_pit_snapshot_maximum_import_rowid": (
                self.bybit_pit_snapshot_maximum_import_rowid
            ),
            "macro_pit_store": (
                str(config.macro_pit_store_path.resolve())
                if config.macro_pit_store_path
                else None
            ),
            "macro_pit_snapshot_maximum_sequence": self.macro_pit_snapshot_maximum_sequence,
            "verify_macro_raw_hashes": config.verify_macro_raw_hashes,
            "flow_pit_store": (
                str(config.flow_pit_store_path.resolve())
                if config.flow_pit_store_path
                else None
            ),
            "flow_pit_snapshot_maximum_sequence": self.flow_pit_snapshot_maximum_sequence,
            "flow_pit_snapshot_maximum_invalidation_rowid": (
                self.flow_pit_snapshot_maximum_invalidation_rowid
            ),
            "verify_flow_raw_hashes": config.verify_flow_raw_hashes,
            "max_bars_per_symbol": config.max_bars_per_symbol,
            "walk_forward_folds": config.walk_forward_folds,
            "lockbox_fraction": config.lockbox_fraction,
            "random_seed": config.random_seed,
            "development_stage_policy": (
                "chronological_disjoint_factor_research_then_frozen_feature_evaluation"
            ),
            "horizons": HORIZONS_SEC,
            "symbols": SYMBOLS,
        }
        self.trial_id = f"profitability_{_hash_payload(run_payload)[:24]}"

    def run(self) -> ProfitabilityGateResult:
        self.ledger.append_event(self.trial_id, "running", {"phase": "load_and_label"})
        panels: dict[int, pd.DataFrame] = {}
        market: dict[str, Sequence[MarketBar]] = {}
        output = self.config.output_dir
        source_evidence: dict[str, object] = {
            "kline_feature_store": {
                **dict(self.feature_store_identity),
                "source": self.config.kline_source,
            }
        }
        coverage_audits: dict[str, dict[str, object]] = {}
        preflight_unique_times_by_horizon: dict[int, pd.Series] = {}
        source_timestamp_counts_by_horizon: dict[int, int] = {}
        for horizon in HORIZONS_SEC:
            timeframe = HORIZON_TIMEFRAME[horizon]
            decision_times: list[pd.Series] = []
            for symbol in SYMBOLS:
                series_id = f"{symbol}:{horizon}"
                frame: pd.DataFrame | None = None
                try:
                    frame = self.source.load_timestamps(
                        symbol, timeframe, self.config.max_bars_per_symbol
                    )
                    audit = audit_source_coverage(
                        frame,
                        timeframe,
                        listing_evidence=self.source.listing_evidence(
                            symbol, timeframe
                        ),
                    )
                    decision_times.append(frame["close_at"].copy())
                except Exception as exc:
                    audit = audit_source_coverage(
                        pd.DataFrame(columns=["open_at", "close_at"]), timeframe
                    )
                    audit["failure_reasons"] = [
                        *list(audit["failure_reasons"]),
                        "source_load_failed",
                    ]
                    audit["load_error"] = f"{type(exc).__name__}: {exc}"
                audit = {
                    "symbol": symbol,
                    "horizon_sec": horizon,
                    "timeframe": timeframe,
                    "decision_sampling": "non_overlapping_max_execution_windows",
                    "paired_side_alternatives": True,
                    **audit,
                }
                coverage_audits[series_id] = audit
                source_evidence[series_id] = audit
                if frame is not None:
                    del frame
            unique_times = (
                pd.concat(decision_times, ignore_index=True)
                .drop_duplicates()
                .sort_values()
                .reset_index(drop=True)
                if decision_times
                else pd.Series(dtype="datetime64[ns, UTC]")
            )
            preflight_unique_times_by_horizon[horizon] = unique_times
            source_timestamp_counts_by_horizon[horizon] = int(len(unique_times))
        _write_kline_data_evidence(
            output,
            trial_id=self.trial_id,
            code_commit=self.config.code_commit,
            feature_store_identity=self.feature_store_identity,
            series_audits=coverage_audits,
            source_timestamp_counts_by_horizon=source_timestamp_counts_by_horizon,
        )
        failed_coverage_series = [
            series_id
            for series_id, audit in coverage_audits.items()
            if audit.get("status") != "PASSED"
        ]
        if failed_coverage_series:
            raise ValueError(
                "kline coverage preflight failed for: "
                + ", ".join(failed_coverage_series)
            )
        verified_bybit_price_series = 0
        for audit in coverage_audits.values():
            evidence = audit.get("listing_evidence")
            if not isinstance(evidence, Mapping):
                continue
            if (
                evidence.get("source") == "bybit"
                and evidence.get("status")
                in {"VERIFIED_WINDOW", "VERIFIED_SINCE_LAUNCH"}
                and evidence.get("raw_receipt_reverified") is True
                and int(evidence.get("immutable_trigger_count", 0)) == 8
            ):
                verified_bybit_price_series += 1
        expected_price_series = len(SYMBOLS) * len(HORIZONS_SEC)
        same_venue_price_evidence = {
            "passed": bool(
                self.config.kline_source == "bybit"
                and verified_bybit_price_series == expected_price_series
            ),
            "status": (
                "PASSED"
                if self.config.kline_source == "bybit"
                and verified_bybit_price_series == expected_price_series
                else "FAILED"
            ),
            "complete": verified_bybit_price_series == expected_price_series,
            "venue": "bybit",
            "price_definition": "official Bybit last-trade kline OHLCV",
            "configured_kline_source": self.config.kline_source,
            "observed_sample_count": verified_bybit_price_series,
            "expected_sample_count": expected_price_series,
            "failed_sample_count": expected_price_series - verified_bybit_price_series,
            "binance_role": "reference_baseline_only_not_release_execution_evidence",
        }
        source_evidence["same_venue_price_evidence"] = same_venue_price_evidence
        bybit_source: BybitPITFeatureSource | None = None
        bybit_evidence_by_horizon: dict[int, dict[str, object]] = {}
        bybit_names: tuple[str, ...] = ()
        bybit_pit_evidence: dict[str, object] | None = None
        execution_bar_evidence: dict[str, object] = {}
        if (
            self.config.bybit_pit_store_path is not None
            or self.config.lockbox_bybit_pit_store_path is not None
        ):
            bybit_names = tuple(
                dict.fromkeys(
                    name
                    for columns in SHORT_FACTOR_GROUPS.values()
                    for name in columns
                )
            )
        if self.config.bybit_pit_store_path is not None:
            bybit_source = BybitPITFeatureSource(self.config.bybit_pit_store_path)
        lockbox_start_by_horizon: dict[int, datetime] = {}
        for horizon in HORIZONS_SEC:
            timeframe = HORIZON_TIMEFRAME[horizon]
            panel_parts: list[pd.DataFrame] = []
            unique_times = preflight_unique_times_by_horizon[horizon]
            if len(unique_times) < 10:
                raise ValueError(f"too few raw decision times for horizon {horizon}")
            boundary_position = min(
                len(unique_times) - 1,
                max(
                    1,
                    int(round(len(unique_times) * (1.0 - self.config.lockbox_fraction))),
                ),
            )
            lockbox_start = pd.Timestamp(unique_times.iloc[boundary_position]).to_pydatetime()
            lockbox_start_by_horizon[horizon] = lockbox_start
            decision_minimum = pd.Timestamp(unique_times.iloc[0]).to_pydatetime()
            max_wait_sec = max(30, min(300, horizon // 2))
            development_decision_end = lockbox_start - timedelta(
                seconds=horizon + max_wait_sec
            )
            bybit_history: pd.DataFrame | None = None
            horizon_evidence: dict[str, object] | None = None
            if bybit_source is not None:
                requested_bybit_names = _bybit_names_for_horizon(
                    horizon, bybit_names
                )
                bybit_history, horizon_evidence = bybit_source.load(
                    requested_bybit_names,
                    maximum_sequence=self.bybit_pit_snapshot_maximum_sequence,
                    maximum_invalidation_rowid=(
                        self.bybit_pit_snapshot_maximum_invalidation_rowid
                    ),
                    maximum_capture_audit_rowid=(
                        self.bybit_pit_snapshot_maximum_capture_audit_rowid
                    ),
                    maximum_pit_import_rowid=(
                        self.bybit_pit_snapshot_maximum_import_rowid
                    ),
                    minimum_decision_at=decision_minimum,
                    maximum_decision_at=development_decision_end,
                    symbols=SYMBOLS,
                )
                bybit_evidence_by_horizon[horizon] = horizon_evidence
                if horizon == 180:
                    bybit_pit_evidence = horizon_evidence
            for symbol in SYMBOLS:
                frame = self.source.load_before(
                    symbol,
                    timeframe,
                    self.config.max_bars_per_symbol,
                    close_at_or_before=lockbox_start,
                    include_boundary=False,
                )
                enriched = _engineer_features(frame)
                development_enriched = enriched.reset_index(drop=True)
                development_bars = _market_bars(development_enriched)
                if bybit_history is not None and horizon_evidence is not None:
                    symbol_history = (
                        bybit_history[
                            bybit_history["symbol"].astype(str).str.upper() == symbol
                        ].copy()
                        if not bybit_history.empty
                        else bybit_history.copy()
                    )
                    development_bars, bar_evidence = enrich_market_bars_with_bybit_execution_pit(
                        development_bars,
                        source=bybit_source,
                        history=symbol_history,
                        source_evidence=horizon_evidence,
                    )
                    execution_bar_evidence[f"{symbol}:{horizon}"] = bar_evidence
                    del symbol_history
                market[f"{symbol}:{horizon}"] = development_bars
                panel_parts.append(
                    _panel_frame(
                        development_enriched,
                        horizon,
                        development_bars,
                        decision_before=development_decision_end,
                    )
                )
                del frame
                del enriched
                del development_enriched
                del development_bars
            panels[horizon] = pd.concat(panel_parts, ignore_index=True)
            panel_parts.clear()
            if bybit_history is not None and horizon in {180, 900}:
                assert bybit_source is not None
                panels[horizon] = bybit_source.join(
                    panels[horizon], names=bybit_names, history=bybit_history
                )
                bybit_history = None
            self.ledger.append_event(
                self.trial_id,
                "running",
                {
                    "phase": "development_horizon_ready",
                    "horizon_sec": horizon,
                    "panel_rows": len(panels[horizon]),
                    "development_decision_end": development_decision_end.isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "sealed_lockbox_start": lockbox_start.isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "bybit_snapshot_rows": (
                        int(bybit_evidence_by_horizon[horizon]["observation_count"])
                        if horizon in bybit_evidence_by_horizon
                        else 0
                    ),
                },
            )
        if bybit_evidence_by_horizon:
            assert bybit_pit_evidence is not None
            bybit_pit_evidence = {
                **bybit_pit_evidence,
                "bounded_development_snapshots": {
                    str(horizon): evidence
                    for horizon, evidence in bybit_evidence_by_horizon.items()
                },
            }
            source_evidence["bybit_public_pit"] = bybit_pit_evidence
        if execution_bar_evidence:
            source_evidence["bybit_execution_bars"] = execution_bar_evidence

        trad_source: TradPanelHistorySource | None = None
        trad_history: pd.DataFrame | None = None
        trad_panel_evidence: dict[str, object] | None = None
        if self.config.trad_panel_root is not None:
            trad_source = TradPanelHistorySource(
                self.config.trad_panel_root,
                verify_sha256=self.config.verify_trad_panel_sha256,
            )
            trad_history, trad_panel_evidence = trad_source.load()
            for horizon in HORIZONS_SEC:
                panels[horizon] = trad_source.join(
                    panels[horizon], history=trad_history
                )
            source_evidence["trad_data_service"] = trad_panel_evidence

        macro_source: MacroPITFeatureSource | None = None
        macro_history: pd.DataFrame | None = None
        macro_pit_evidence: dict[str, object] | None = None
        if self.config.macro_pit_store_path is not None:
            macro_names = tuple(MACRO_FEATURE_CONTRACTS)
            macro_source = MacroPITFeatureSource(
                self.config.macro_pit_store_path,
                verify_raw_hashes=self.config.verify_macro_raw_hashes,
            )
            macro_history, macro_pit_evidence = macro_source.load(
                macro_names,
                maximum_sequence=self.macro_pit_snapshot_maximum_sequence,
            )
            for horizon in HORIZONS_SEC:
                panels[horizon] = macro_source.join(
                    panels[horizon], names=macro_names, history=macro_history
                )
            source_evidence["fred_alfred_pit"] = macro_pit_evidence

        flow_source: FlowPITFeatureSource | None = None
        flow_history: pd.DataFrame | None = None
        flow_pit_evidence: dict[str, object] | None = None
        if self.config.flow_pit_store_path is not None:
            flow_names = tuple(FLOW_FEATURE_CONTRACTS)
            flow_source = FlowPITFeatureSource(
                self.config.flow_pit_store_path,
                verify_raw_hashes=self.config.verify_flow_raw_hashes,
            )
            flow_history, flow_pit_evidence = flow_source.load(
                flow_names,
                maximum_sequence=self.flow_pit_snapshot_maximum_sequence,
                maximum_invalidation_rowid=(
                    self.flow_pit_snapshot_maximum_invalidation_rowid
                ),
            )
            for horizon in HORIZONS_SEC:
                panels[horizon] = flow_source.join(
                    panels[horizon], names=flow_names, history=flow_history
                )
            source_evidence["flow_pit"] = flow_pit_evidence

        splitter = PooledPanelBuilder(
            lockbox_fraction=self.config.lockbox_fraction,
            minimum_train_rows=300,
            minimum_test_rows=80,
            maximum_folds=self.config.walk_forward_folds,
        )
        datasets: dict[int, object] = {}
        release_datasets: dict[int, object] = {}
        release_dataset_evidence: dict[int, dict[str, object]] = {}
        for horizon in HORIZONS_SEC:
            horizon_panel = panels.pop(horizon)
            datasets[horizon] = splitter.build_sealed_development(
                horizon_panel,
                horizon,
                lockbox_start=lockbox_start_by_horizon[horizon],
            )
            release_dataset, direct_evidence = _build_direct_release_dataset(
                splitter,
                horizon_panel,
                horizon,
                lockbox_start=lockbox_start_by_horizon[horizon],
            )
            if release_dataset is not None:
                release_datasets[horizon] = release_dataset
            release_dataset_evidence[horizon] = direct_evidence
            del horizon_panel
        source_evidence["direct_execution_release_development"] = {
            str(horizon): evidence
            for horizon, evidence in release_dataset_evidence.items()
        }
        factor_research_datasets: dict[int, HorizonDataset] = {}
        evaluation_datasets: dict[int, HorizonDataset] = {}
        factor_research_release_datasets: dict[int, HorizonDataset] = {}
        evaluation_release_datasets: dict[int, HorizonDataset] = {}
        development_stage_partitions: dict[str, dict[str, object]] = {}
        for horizon, dataset in datasets.items():
            research, evaluation, evidence = (
                split_factor_research_and_evaluation(dataset)
            )
            factor_research_datasets[horizon] = research
            evaluation_datasets[horizon] = evaluation
            development_stage_partitions[str(horizon)] = {
                "full_panel": evidence,
                "direct_execution_release": None,
            }
        for horizon, dataset in release_datasets.items():
            research, evaluation, evidence = (
                split_factor_research_and_evaluation(dataset)
            )
            factor_research_release_datasets[horizon] = research
            evaluation_release_datasets[horizon] = evaluation
            development_stage_partitions[str(horizon)][
                "direct_execution_release"
            ] = evidence
            release_dataset_evidence[horizon]["stage_partition"] = evidence
        datasets = evaluation_datasets
        release_datasets = evaluation_release_datasets
        source_evidence["development_oos_stage_partition"] = (
            development_stage_partitions
        )
        panels.clear()
        del panels
        current_feature_store_stat = self.config.feature_store_path.stat()
        if (
            current_feature_store_stat.st_size,
            current_feature_store_stat.st_mtime_ns,
        ) != self.feature_store_snapshot:
            raise RuntimeError(
                "kline feature store changed after the development snapshot was frozen"
            )
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "labels_and_pooled_panels_ready",
                "datasets": {
                    str(horizon): dataset_manifest(dataset)
                    for horizon, dataset in datasets.items()
                },
                "release_datasets": {
                    str(horizon): evidence
                    for horizon, evidence in release_dataset_evidence.items()
                },
                "factor_research_datasets": {
                    str(horizon): dataset_manifest(dataset)
                    for horizon, dataset in factor_research_datasets.items()
                },
                "frozen_feature_evaluation_datasets": {
                    str(horizon): dataset_manifest(dataset)
                    for horizon, dataset in evaluation_datasets.items()
                },
                "development_oos_stage_partition": development_stage_partitions,
            },
        )
        walk_forward: list[dict[str, object]] = []
        development_signals_by_horizon: dict[int, list[SignalEvent]] = {
            horizon: [] for horizon in HORIZONS_SEC
        }
        candidate_configs = (
            TwoStageConfig(
                direction_iterations=80,
                meta_iterations=80,
                learning_rate=0.03,
                l2=0.03,
                ridge=1.0,
                tail_penalty=0.75,
                meta_trade_probability=0.58,
            ),
            TwoStageConfig(
                direction_iterations=80,
                meta_iterations=80,
                learning_rate=0.03,
                l2=0.03,
                ridge=2.0,
                tail_penalty=0.75,
                meta_trade_probability=0.62,
            ),
        )
        candidate_config_ids = {
            _hash_payload(asdict(config))[:16]: config for config in candidate_configs
        }
        historical_pipeline_trial_count = self.ledger.trial_count(
            "profitability_two_stage"
        )
        statistical_trial_audit = _precommitted_statistical_trial_count(
            len(candidate_configs), historical_pipeline_trial_count
        )
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "statistical_governance_preregistered",
                **statistical_trial_audit,
                "candidate_config_ids": sorted(candidate_config_ids),
                "dsr_minimum_probability": 0.95,
                "cscv_maximum_pbo": 0.05,
                "cscv_partitions": 8,
            },
        )
        selector = NestedWalkForwardSelector(candidate_configs, inner_folds=3)
        backtest = EventDrivenBacktest(BacktestConfig())
        ablation_backtest = EventDrivenBacktest(
            BacktestConfig(require_positive_lower_bound_edge=False)
        )
        evaluated_factor_groups: dict[str, dict[str, object]] = {}

        def record_ablation_progress(payload: Mapping[str, object]) -> None:
            self.ledger.append_event(
                self.trial_id,
                "running",
                {
                    "phase": "factor_ablation_fold_progress",
                    **dict(payload),
                },
            )

        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "factor_ablation_group_started",
                "factor_group": "legacy_brain_technical",
            },
        )
        legacy_result = _evaluate_legacy_technical_ablation(
            factor_research_release_datasets,
            market,
            selector,
            ablation_backtest,
            progress_callback=record_ablation_progress,
        )
        legacy_result["legacy_brain_technical"] = _horizon_scoped_ablation_result(
            legacy_result["legacy_brain_technical"], HORIZONS_SEC
        )
        evaluated_factor_groups.update(legacy_result)
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "factor_ablation_group_completed",
                "factor_group": "legacy_brain_technical",
                "result": _ablation_ledger_summary(
                    legacy_result["legacy_brain_technical"]
                ),
            },
        )
        if (
            trad_panel_evidence is not None
            or macro_pit_evidence is not None
            or flow_pit_evidence is not None
        ):
            for group, columns in LONG_FACTOR_GROUPS.items():
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "factor_ablation_group_started",
                        "factor_group": group,
                    },
                )
                result = _evaluate_long_factor_ablation(
                    factor_research_release_datasets,
                    market,
                    selector,
                    ablation_backtest,
                    factor_groups={group: columns},
                    progress_callback=record_ablation_progress,
                )
                result[group] = _horizon_scoped_ablation_result(
                    result[group], (7200, 14400, 86400)
                )
                evaluated_factor_groups.update(result)
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "factor_ablation_group_completed",
                        "factor_group": group,
                        "result": _ablation_ledger_summary(result[group]),
                    },
                )
        if bybit_pit_evidence is not None:
            for group, columns in SHORT_FACTOR_GROUPS.items():
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "factor_ablation_group_started",
                        "factor_group": group,
                    },
                )
                result = _evaluate_bybit_pit_ablation(
                    factor_research_release_datasets,
                    market,
                    selector,
                    ablation_backtest,
                    bybit_pit_evidence,
                    factor_groups={group: columns},
                    progress_callback=record_ablation_progress,
                )
                result[group] = _horizon_scoped_ablation_result(
                    result[group], (180, 900)
                )
                evaluated_factor_groups.update(result)
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "factor_ablation_group_completed",
                        "factor_group": group,
                        "result": _ablation_ledger_summary(result[group]),
                    },
                )
        factor_report = _factor_ablation_report(evaluated_factor_groups)
        factor_report["oos_stage_partition"] = development_stage_partitions
        factor_report["factor_selection_scope"] = (
            "chronologically_earlier_factor_research_oos_only"
        )
        factor_report["frozen_feature_evaluation_oos_used_for_selection"] = False
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "factor_ablation_ready",
                "all_required_groups_evaluated": factor_report[
                    "all_required_groups_evaluated"
                ],
                "retained_factor_groups": factor_report[
                    "retained_factor_groups"
                ],
            },
        )
        retained_groups = tuple(
            str(group) for group in factor_report["retained_factor_groups"]
        )
        factor_results_by_group = {
            str(item["factor_group"]): item
            for item in factor_report["groups"]
        }
        model_feature_columns_by_horizon: dict[int, tuple[str, ...]] = {}
        for horizon in HORIZONS_SEC:
            retained_factor_columns: list[str] = []
            for group in retained_groups:
                retained_horizons = {
                    int(value)
                    for value in factor_results_by_group[group].get(
                        "retained_horizons", []
                    )
                }
                if horizon not in retained_horizons:
                    continue
                if group in LEGACY_FACTOR_GROUPS:
                    retained_factor_columns.extend(LEGACY_FACTOR_GROUPS[group])
                if horizon in {180, 900} and group in SHORT_FACTOR_GROUPS:
                    retained_factor_columns.extend(SHORT_FACTOR_GROUPS[group])
                if horizon >= 7200 and group in LONG_FACTOR_GROUPS:
                    retained_factor_columns.extend(LONG_FACTOR_GROUPS[group])
            model_feature_columns_by_horizon[horizon] = FEATURE_COLUMNS + tuple(
                dict.fromkeys(retained_factor_columns)
            )
        variant_signals_by_horizon: dict[
            int, dict[str, list[SignalEvent]]
        ] = {
            horizon: {config_id: [] for config_id in candidate_config_ids}
            for horizon in HORIZONS_SEC
        }
        development_evaluation_timestamps_by_horizon: dict[int, list[object]] = {
            horizon: [] for horizon in HORIZONS_SEC
        }
        development_calibration_rows_by_horizon: dict[
            int, list[dict[str, object]]
        ] = {horizon: [] for horizon in HORIZONS_SEC}
        for horizon, dataset in release_datasets.items():
            model_feature_columns = model_feature_columns_by_horizon[horizon]
            for fold in dataset.folds:
                train = dataset.development.iloc[fold.train_indices]
                test = dataset.development.iloc[fold.test_indices]
                selection = selector.select_and_fit(train, model_feature_columns)
                predictions = selection.model.predict(test)
                signals = _signals_from_predictions(test, predictions, horizon)
                prediction_gate = prediction_gate_diagnostics(
                    test,
                    predictions,
                    meta_threshold=selection.selected_config.meta_trade_probability,
                )
                development_signals_by_horizon[horizon].extend(signals)
                report = backtest.run(signals, market)
                development_evaluation_timestamps_by_horizon[horizon].extend(
                    test["decision_at"].tolist()
                )
                development_calibration_rows_by_horizon[horizon].extend(
                    directional_calibration_rows(test, predictions)
                )
                selected_config_id = _hash_payload(
                    asdict(selection.selected_config)
                )[:16]
                for config_id, config in candidate_config_ids.items():
                    if config_id == selected_config_id:
                        variant_predictions = predictions
                        variant_signals = signals
                        variant_report = report
                    else:
                        variant_model = TwoStageAlphaModel(config).fit(
                            train, model_feature_columns
                        )
                        variant_predictions = variant_model.predict(test)
                        variant_signals = _signals_from_predictions(
                            test, variant_predictions, horizon
                        )
                        variant_report = backtest.run(variant_signals, market)
                    variant_signals_by_horizon[horizon][config_id].extend(
                        variant_signals
                    )
                    self.ledger.append_event(
                        self.trial_id,
                        "running",
                        {
                            "phase": "outer_walk_forward_variant_scored",
                            "horizon_sec": horizon,
                            "fold_id": fold.fold_id,
                            "config_id": config_id,
                            "selected_by_inner_oos": config_id == selected_config_id,
                            "outer_oos_used_for_tuning": False,
                            "signals": len(variant_signals),
                            "trades": len(variant_report.trades),
                            "net_return": variant_report.net_return,
                        },
                    )
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "outer_walk_forward_fold_scored",
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "signals": len(signals),
                        "trades": len(report.trades),
                        "net_return": report.net_return,
                    },
                )
                walk_forward.append(
                    {
                        "horizon_sec": horizon,
                        "fold_id": fold.fold_id,
                        "train_rows": len(train),
                        "test_rows": len(test),
                        "signals": len(signals),
                        "prediction_gate": prediction_gate,
                        "trades": len(report.trades),
                        "net_return": report.net_return,
                        "max_drawdown": report.max_drawdown,
                        "profit_factor": report.profit_factor,
                        "purge_sec": fold.purge_sec,
                        "embargo_sec": fold.embargo_sec,
                        "nested_selection": dict(selection.audit),
                        "inner_candidate_results": list(selection.candidate_results),
                        "outer_oos_used_for_tuning": False,
                        "statistical_variant_config_ids": sorted(
                            candidate_config_ids
                        ),
                        "formal_feature_columns": list(model_feature_columns),
                    }
                )

        development_calibration_by_horizon = {
            horizon: evaluate_quantile_coverage(
                development_calibration_rows_by_horizon[horizon],
                required_horizons=[horizon],
            )
            for horizon in HORIZONS_SEC
        }
        development_calibration_evidence = evaluate_quantile_coverage(
            [
                row
                for horizon in HORIZONS_SEC
                for row in development_calibration_rows_by_horizon[horizon]
            ],
            required_horizons=HORIZONS_SEC,
        )
        horizon_development_gates: dict[int, ProfitabilityGateResult] = {}
        horizon_development_reports: dict[int, dict[str, object]] = {}
        horizon_development_statistical_evidence: dict[int, dict[str, object]] = {}
        horizon_variant_reports: dict[int, dict[str, object]] = {}
        for horizon in HORIZONS_SEC:
            horizon_signals = development_signals_by_horizon[horizon]
            horizon_report = backtest.run(horizon_signals, market)
            horizon_stress = backtest.run(
                horizon_signals, market, cost_multiplier=2.0
            )
            horizon_execution_evidence = _execution_release_evidence(
                horizon_report
            )
            variant_reports = {
                config_id: backtest.run(signals, market)
                for config_id, signals in variant_signals_by_horizon[horizon].items()
            }
            horizon_variant_reports[horizon] = variant_reports
            horizon_statistical_evidence = statistical_overfit_evidence(
                horizon_report,
                tuple(variant_reports[config_id] for config_id in sorted(variant_reports)),
                development_evaluation_timestamps_by_horizon[horizon],
                number_of_trials=int(statistical_trial_audit["number_of_trials"]),
            )
            horizon_development_statistical_evidence[horizon] = (
                horizon_statistical_evidence
            )
            horizon_gate = evaluate_development_gate(
                horizon_report.trades,
                [
                    fold
                    for fold in walk_forward
                    if int(fold["horizon_sec"]) == horizon
                ],
                initial_equity_usdt=horizon_report.initial_equity_usdt,
                two_x_cost_net_return=horizon_stress.net_return,
                mark_to_market_max_drawdown=horizon_report.max_drawdown,
                mark_to_market_evidence_complete=horizon_report.mark_to_market_used,
                execution_evidence_complete=bool(
                    horizon_execution_evidence[
                        "candidate_backtest_execution_evidence_complete"
                    ]
                ),
                factor_ablation_complete=bool(
                    factor_report["all_required_groups_evaluated"]
                ),
                statistical_overfit_evidence=horizon_statistical_evidence,
                calibration_coverage_evidence=(
                    development_calibration_by_horizon[horizon]
                ),
                gate_scope="horizon",
                thresholds=ProfitabilityThresholds(),
            )
            horizon_development_gates[horizon] = horizon_gate
            horizon_development_reports[horizon] = {
                "gate": horizon_gate.to_dict(),
                "normal_cost": horizon_report.to_dict(include_trades=False),
                "two_x_cost": horizon_stress.to_dict(include_trades=False),
                "direct_execution_release_dataset": (
                    release_dataset_evidence[horizon]
                ),
                "statistical_overfit_evidence": horizon_statistical_evidence,
                "pre_registered_variant_results": {
                    config_id: report.to_dict(include_trades=False)
                    for config_id, report in variant_reports.items()
                },
            }
            self.ledger.append_event(
                self.trial_id,
                "running",
                {
                    "phase": "development_horizon_gate_scored",
                    "horizon_sec": horizon,
                    "profitability_gate": horizon_gate.profitability_gate,
                    "blockers": list(horizon_gate.blockers),
                    "deflated_sharpe_probability": horizon_statistical_evidence.get(
                        "deflated_sharpe_probability"
                    ),
                    "cscv_pbo": horizon_statistical_evidence.get(
                        "probability_of_backtest_overfitting"
                    ),
                },
            )
        development_eligible_horizons = tuple(
            horizon
            for horizon in HORIZONS_SEC
            if horizon_development_gates[horizon].passed
        )
        development_signals = [
            signal
            for horizon in development_eligible_horizons
            for signal in development_signals_by_horizon[horizon]
        ]
        eligible_walk_forward = [
            fold
            for fold in walk_forward
            if int(fold["horizon_sec"]) in development_eligible_horizons
        ]
        development_report = backtest.run(development_signals, market)
        development_stress = backtest.run(
            development_signals, market, cost_multiplier=2.0
        )
        execution_evidence = _execution_release_evidence(development_report)
        candidate_execution_evidence_complete = bool(
            execution_evidence[
                "candidate_backtest_execution_evidence_complete"
            ]
        )
        portfolio_variant_reports = {
            config_id: backtest.run(
                [
                    signal
                    for horizon in development_eligible_horizons
                    for signal in variant_signals_by_horizon[horizon][config_id]
                ],
                market,
            )
            for config_id in sorted(candidate_config_ids)
        }
        portfolio_evaluation_timestamps = [
            value
            for horizon in development_eligible_horizons
            for value in development_evaluation_timestamps_by_horizon[horizon]
        ]
        development_statistical_evidence = statistical_overfit_evidence(
            development_report,
            tuple(
                portfolio_variant_reports[config_id]
                for config_id in sorted(portfolio_variant_reports)
            ),
            portfolio_evaluation_timestamps,
            number_of_trials=int(statistical_trial_audit["number_of_trials"]),
        )
        development_gate = evaluate_development_gate(
            development_report.trades,
            eligible_walk_forward,
            initial_equity_usdt=development_report.initial_equity_usdt,
            two_x_cost_net_return=development_stress.net_return,
            mark_to_market_max_drawdown=development_report.max_drawdown,
            mark_to_market_evidence_complete=development_report.mark_to_market_used,
            execution_evidence_complete=candidate_execution_evidence_complete,
            factor_ablation_complete=bool(factor_report["all_required_groups_evaluated"]),
            statistical_overfit_evidence=development_statistical_evidence,
            calibration_coverage_evidence=development_calibration_evidence,
            thresholds=ProfitabilityThresholds(),
        )
        development_gate = _require_development_evidence(
            development_gate,
            check_name="bybit_same_venue_price_path",
            evidence=same_venue_price_evidence,
        )
        development_nested_cv_evidence = nested_cv_evidence(walk_forward)
        development_signal_funnel_evidence = signal_funnel_evidence(
            eligible_walk_forward,
            development_report,
            scope="development_outer_oos",
        )
        development_intratrade_drawdown_evidence = intratrade_drawdown_evidence(
            development_report,
            scope="development_outer_oos",
        )
        oos_timestamp_evidence = {}
        for horizon in HORIZONS_SEC:
            timestamps = pd.to_datetime(
                pd.Series(development_evaluation_timestamps_by_horizon[horizon]),
                utc=True,
                errors="coerce",
            ).dropna()
            unique_timestamp_count = int(timestamps.nunique())
            oos_timestamp_evidence[horizon] = {
                "outer_oos_prediction_row_count": int(len(timestamps)),
                "unique_decision_timestamp_count": unique_timestamp_count,
                "non_independent_duplicate_row_count": int(
                    len(timestamps) - unique_timestamp_count
                ),
                "unique_utc_day_count": int(timestamps.dt.floor("D").nunique()),
                "outer_fold_count": sum(
                    1
                    for fold in walk_forward
                    if int(fold["horizon_sec"]) == horizon
                ),
                "paired_side_alternatives_counted_once": True,
                "simultaneous_symbols_counted_once": True,
                "overlapping_execution_windows_allowed": False,
            }
        _write_kline_data_evidence(
            output,
            trial_id=self.trial_id,
            code_commit=self.config.code_commit,
            feature_store_identity=self.feature_store_identity,
            series_audits=coverage_audits,
            source_timestamp_counts_by_horizon=source_timestamp_counts_by_horizon,
            oos_timestamp_evidence=oos_timestamp_evidence,
        )
        _archive_candidate_manifest(output, self.trial_id)
        write_profitability_report(output / "profitability_report.json", development_gate)
        _atomic_json(
            output / "calibration_coverage_report.json",
            {
                "schema_version": "profitability-calibration-release.v1",
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "status": "LOCKBOX_NOT_EVALUATED",
                "complete": False,
                "development": {
                    "portfolio": development_calibration_evidence,
                    "horizons": {
                        str(horizon): evidence
                        for horizon, evidence in development_calibration_by_horizon.items()
                    },
                },
                "lockbox": {
                    "status": "SEALED_NOT_OPENED",
                    "used_for_calibration_or_tuning": False,
                },
            },
        )
        _atomic_json(
            output / "nested_cv_report.json",
            {
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                **development_nested_cv_evidence,
            },
        )
        _atomic_json(
            output / "signal_funnel_report.json",
            {
                "schema_version": "profitability-signal-funnel.v1",
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "status": "LOCKBOX_NOT_EVALUATED",
                "complete": False,
                "development": development_signal_funnel_evidence,
                "lockbox": {"status": "SEALED_NOT_OPENED"},
            },
        )
        _atomic_json(
            output / "intratrade_drawdown_report.json",
            {
                "schema_version": "profitability-intratrade-drawdown.v1",
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "status": "LOCKBOX_NOT_EVALUATED",
                "complete": False,
                "development": development_intratrade_drawdown_evidence,
                "lockbox": {"status": "SEALED_NOT_OPENED"},
            },
        )
        _atomic_json(
            output / "walk_forward_report.json",
            {
                "trial_id": self.trial_id,
                "method": "nested pooled-panel walk-forward; inner OOS selects parameters, outer OOS scores once",
                "outer_oos_used_for_tuning": False,
                "folds": walk_forward,
                "development_horizon_gates": {
                    str(horizon): report
                    for horizon, report in horizon_development_reports.items()
                },
                "development_eligible_horizons": list(
                    development_eligible_horizons
                ),
                "direct_execution_release_datasets": {
                    str(horizon): evidence
                    for horizon, evidence in release_dataset_evidence.items()
                },
                "candidate_horizon_selection_source": (
                    "frozen_feature_development_evaluation_oos_before_lockbox"
                ),
                "factor_research_datasets": {
                    str(h): dataset_manifest(ds)
                    for h, ds in factor_research_datasets.items()
                },
                "frozen_feature_evaluation_datasets": {
                    str(h): dataset_manifest(ds) for h, ds in datasets.items()
                },
                "development_oos_stage_partition": development_stage_partitions,
                "positive_fold_ratio": development_gate.metrics[
                    "positive_walk_forward_fold_ratio"
                ],
                "development_portfolio": development_report.to_dict(include_trades=True),
                "datasets": {str(h): dataset_manifest(ds) for h, ds in datasets.items()},
            },
        )
        _atomic_json(output / "factor_ablation_report.json", factor_report)
        _atomic_json(
            output / "statistical_overfit_report.json",
            {
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "data_snapshot_fingerprint": _hash_payload(source_evidence),
                "feature_schema_hash": _hash_payload(
                    model_feature_columns_by_horizon
                ),
                "statistical_policy_hash": _hash_payload(
                    {
                        "candidate_configs": [
                            asdict(config) for config in candidate_configs
                        ],
                        "trial_count_audit": statistical_trial_audit,
                        "dsr_minimum_probability": 0.95,
                        "cscv_maximum_pbo": 0.05,
                        "cscv_partitions": 8,
                    }
                ),
                "evaluation_scope": "development_outer_oos",
                "thresholds": {
                    "minimum_deflated_sharpe_probability": 0.95,
                    "maximum_cscv_probability_of_backtest_overfitting": 0.05,
                },
                "trial_count_audit": statistical_trial_audit,
                "portfolio": development_statistical_evidence,
                "horizons": {
                    str(horizon): evidence
                    for horizon, evidence in horizon_development_statistical_evidence.items()
                },
                "lockbox_policy": (
                    "DSR is recomputed once on the selected lockbox path; CSCV/PBO remains "
                    "frozen on development and alternative variants are never scored on lockbox"
                ),
                "sources": [
                    "https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf",
                    "https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf",
                ],
            },
        )
        _atomic_json(
            output / "execution_cost_report.json",
            {
                "evaluation_scope": "development_oos",
                "execution_evidence_complete": candidate_execution_evidence_complete,
                "candidate_backtest_execution_evidence_complete": (
                    candidate_execution_evidence_complete
                ),
                "live_execution_evidence_complete": bool(
                    execution_evidence["live_execution_evidence_complete"]
                ),
                "execution_evidence": execution_evidence,
                "normal_cost": development_report.to_dict(include_trades=False),
                "two_x_cost": development_stress.to_dict(include_trades=False),
                "limitations": [
                    *(
                        []
                        if development_report.proxy_execution_cost_trade_count == 0
                        else [
                            "one or more trades still use OHLCV-derived execution cost proxies"
                        ]
                    ),
                    *(
                        []
                        if bybit_pit_evidence is not None
                        else ["no PIT Bybit public execution source was supplied"]
                    ),
                    "official historical public data is not realized own-order fill evidence",
                    "immutable OOS shadow/testnet receipts and queue/latency calibration are incomplete",
                    "candidate evidence never authorizes live execution; live remains separately fail-closed",
                ],
            },
        )
        _atomic_json(
            output / "capital_preservation_report.json",
            policy_report(CapitalPreservationConfig()),
        )

        # A rejected model is still useful for auditable shadow collection.
        # It is fitted on development only and cannot promote itself.  Saving
        # it here does not inspect or consume the sealed lockbox.
        model_dir = self.config.model_output_dir / self.trial_id
        model_paths: dict[str, str] = {}
        model_sha256: dict[str, str] = {}
        final_selection: dict[str, object] = {}
        final_models: dict[int, TwoStageAlphaModel] = {}
        for horizon, dataset in datasets.items():
            model_feature_columns = model_feature_columns_by_horizon[horizon]
            training_dataset = release_datasets.get(horizon, dataset)
            selection = selector.select_and_fit(
                training_dataset.development, model_feature_columns
            )
            path = model_dir / f"horizon_{horizon}.json"
            selection.model.save(path)
            final_models[horizon] = selection.model
            model_paths[str(horizon)] = path.name
            model_sha256[str(horizon)] = _sha256_file(path)
            final_selection[str(horizon)] = {
                "audit": dict(selection.audit),
                "candidate_results": list(selection.candidate_results),
                "training_scope": (
                    "direct_execution_release_development"
                    if horizon in release_datasets
                    else "rejected_shadow_full_development"
                ),
            }
        bundle_path = model_dir / "model_bundle.json"
        rejected_bundle = {
            "schema_version": "profitability-model-bundle.v2",
            "trial_id": self.trial_id,
            "model_family": "profitability_two_stage",
            "kline_source": self.config.kline_source,
            "price_venue": self.config.kline_source,
            "release_stage": "rejected",
            "profitability_gate": "FAILED",
            "models": model_paths,
            "model_sha256": model_sha256,
            "formal_feature_columns": {
                str(horizon): list(columns)
                for horizon, columns in model_feature_columns_by_horizon.items()
            },
            "retained_factor_groups": list(retained_groups),
            "approved_horizons": [],
            "development_eligible_horizons": list(
                development_eligible_horizons
            ),
            "candidate_horizon_selection_source": (
                "frozen_feature_development_evaluation_oos_before_lockbox"
            ),
            "lockbox_fingerprint": None,
            "lockbox_start_by_horizon": {
                str(horizon): value.isoformat().replace("+00:00", "Z")
                for horizon, value in lockbox_start_by_horizon.items()
            },
            "lockbox_consumed": False,
            "code_commit": self.config.code_commit,
        }
        _atomic_json(bundle_path, rejected_bundle)

        replay_snapshot_watermarks: dict[str, Mapping[str, int | None]] = {
            "bybit": {
                "maximum_sequence": self.bybit_pit_snapshot_maximum_sequence,
                "maximum_invalidation_rowid": (
                    self.bybit_pit_snapshot_maximum_invalidation_rowid
                ),
            },
            "macro": {
                "maximum_sequence": self.macro_pit_snapshot_maximum_sequence,
            },
            "flow": {
                "maximum_sequence": self.flow_pit_snapshot_maximum_sequence,
                "maximum_invalidation_rowid": (
                    self.flow_pit_snapshot_maximum_invalidation_rowid
                ),
            },
        }
        replay_evidence = _run_production_replay(
            source=self.source,
            max_bars_per_symbol=self.config.max_bars_per_symbol,
            release_datasets=release_datasets,
            final_models=final_models,
            model_feature_columns_by_horizon=model_feature_columns_by_horizon,
            model_bundle_path=bundle_path,
            trad_panel_evidence=trad_panel_evidence,
            bybit_pit_store_path=self.config.bybit_pit_store_path,
            macro_pit_store_path=self.config.macro_pit_store_path,
            flow_pit_store_path=self.config.flow_pit_store_path,
            pit_snapshot_watermarks=replay_snapshot_watermarks,
        )
        _atomic_json(
            output / "production_replay_report.json",
            {
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "replayed_model_bundle_sha256": _sha256_file(bundle_path),
                "replayed_model_sha256": dict(model_sha256),
                "replayed_feature_contract_sha256": _hash_payload(
                    model_feature_columns_by_horizon
                ),
                "final_model_bundle_sha256": None,
                "final_bundle_models_match_replayed": None,
                **replay_evidence,
            },
        )
        development_gate = _require_development_evidence(
            development_gate,
            check_name="production_replay",
            evidence=replay_evidence,
        )
        write_profitability_report(
            output / "profitability_report.json", development_gate
        )

        if not development_gate.passed:
            _atomic_json(
                output / "lockbox_report.json",
                {
                    "trial_id": self.trial_id,
                    "status": "SEALED_NOT_OPENED",
                    "lockbox_evaluated": False,
                    "lockbox_labels_materialized": False,
                    "used_for_parameter_selection": False,
                    "lockbox_start_by_horizon": {
                        str(horizon): value.isoformat().replace("+00:00", "Z")
                        for horizon, value in lockbox_start_by_horizon.items()
                    },
                    "reason": "development profitability, factor, or execution gate failed",
                    "source_evidence": source_evidence,
                    "rejected_shadow_model_bundle": str(bundle_path),
                },
            )
            record = TrialRecord(
                trial_id=self.trial_id,
                model_family="profitability_two_stage",
                data_signature=_hash_payload(source_evidence)[:24],
                parameter_hash=TrialLedger.parameter_hash(
                    {
                        "candidate_configs": [asdict(config) for config in candidate_configs],
                        "features_by_horizon": model_feature_columns_by_horizon,
                        "nested_walk_forward": True,
                    }
                ),
                code_commit=self.config.code_commit,
                status="rejected",
                metrics=development_gate.to_dict(),
            )
            self.ledger.append(record)
            self.ledger.append_event(self.trial_id, "rejected", development_gate.to_dict())
            return development_gate

        current_feature_store_stat = self.config.feature_store_path.stat()
        if (
            current_feature_store_stat.st_size,
            current_feature_store_stat.st_mtime_ns,
        ) != self.feature_store_snapshot:
            raise RuntimeError(
                "kline feature store changed before the lockbox snapshot was opened"
            )
        lockbox_bybit_source = bybit_source
        lockbox_bybit_maximum_sequence = self.bybit_pit_snapshot_maximum_sequence
        lockbox_bybit_maximum_invalidation_rowid = (
            self.bybit_pit_snapshot_maximum_invalidation_rowid
        )
        lockbox_bybit_maximum_capture_audit_rowid = (
            self.bybit_pit_snapshot_maximum_capture_audit_rowid
        )
        lockbox_bybit_maximum_import_rowid = (
            self.bybit_pit_snapshot_maximum_import_rowid
        )
        lockbox_bybit_snapshot: dict[str, object] = {
            "policy": "reuse_frozen_development_snapshot",
            "database": (
                str(bybit_source.path) if bybit_source is not None else None
            ),
            "snapshot_maximum_sequence": lockbox_bybit_maximum_sequence,
            "snapshot_maximum_invalidation_rowid": (
                lockbox_bybit_maximum_invalidation_rowid
            ),
            "snapshot_maximum_capture_audit_rowid": (
                lockbox_bybit_maximum_capture_audit_rowid
            ),
            "snapshot_maximum_import_rowid": lockbox_bybit_maximum_import_rowid,
        }
        if self.config.lockbox_bybit_pit_store_path is not None:
            # This store is deliberately not instantiated, stat-ed, or queried
            # until the development profitability gate has passed.  Its frozen
            # sequence can therefore never influence model/factor selection.
            lockbox_bybit_source = BybitPITFeatureSource(
                self.config.lockbox_bybit_pit_store_path
            )
            (
                lockbox_bybit_maximum_sequence,
                lockbox_bybit_maximum_invalidation_rowid,
            ) = lockbox_bybit_source.snapshot_watermarks()
            (
                lockbox_bybit_maximum_capture_audit_rowid,
                lockbox_bybit_maximum_import_rowid,
            ) = lockbox_bybit_source.evidence_watermarks()
            lockbox_bybit_snapshot = {
                "policy": "separate_post_development_snapshot",
                "database": str(lockbox_bybit_source.path),
                "snapshot_maximum_sequence": lockbox_bybit_maximum_sequence,
                "snapshot_maximum_invalidation_rowid": (
                    lockbox_bybit_maximum_invalidation_rowid
                ),
                "snapshot_maximum_capture_audit_rowid": (
                    lockbox_bybit_maximum_capture_audit_rowid
                ),
                "snapshot_maximum_import_rowid": (
                    lockbox_bybit_maximum_import_rowid
                ),
            }
        lockbox_kline_identity = _stable_file_identity(
            self.config.feature_store_path
        )
        if (
            int(lockbox_kline_identity["size_bytes"]),
            int(lockbox_kline_identity["modified_ns"]),
        ) != self.feature_store_snapshot:
            raise RuntimeError(
                "kline feature store changed before the lockbox snapshot was hashed"
            )
        kline_snapshot_sha256 = str(lockbox_kline_identity["sha256"])
        if kline_snapshot_sha256 != str(self.feature_store_identity["sha256"]):
            raise RuntimeError(
                "kline feature store content changed before lockbox evaluation"
            )
        lockbox_source_identity = {
            "kline_feature_store": lockbox_kline_identity,
            "bybit_public_pit": lockbox_bybit_snapshot,
            "macro_pit_snapshot_maximum_sequence": (
                self.macro_pit_snapshot_maximum_sequence
            ),
            "flow_pit_snapshot_maximum_sequence": (
                self.flow_pit_snapshot_maximum_sequence
            ),
            "flow_pit_snapshot_maximum_invalidation_rowid": (
                self.flow_pit_snapshot_maximum_invalidation_rowid
            ),
            "lockbox_start_by_horizon": {
                str(horizon): value.isoformat().replace("+00:00", "Z")
                for horizon, value in lockbox_start_by_horizon.items()
            },
        }
        lockbox_claim_identity = {
            "kline_feature_store_sha256": kline_snapshot_sha256,
            "scope": "all_sealed_lockbox_paths_in_snapshot",
        }
        # The claim key identifies the immutable label source, not the trial,
        # path, boundary choice, or auxiliary features.  Thus copying the same
        # database, moving the boundary, or changing execution factors cannot
        # make already-consumed outcomes into a supposedly new lockbox.
        sealed_lockbox_descriptor = _hash_payload(lockbox_claim_identity)
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "lockbox_snapshot_frozen_after_development_pass",
                "walk_forward_folds": len(walk_forward),
                "sealed_lockbox_descriptor": sealed_lockbox_descriptor,
                "lockbox_claim_identity": lockbox_claim_identity,
                "lockbox_source_identity": lockbox_source_identity,
            },
        )
        self.ledger.claim_lockbox(
            sealed_lockbox_descriptor, self.trial_id, purpose="final_evaluation"
        )
        self.ledger.append_event(
            self.trial_id,
            "running",
            {
                "phase": "open_new_lockbox_after_development_pass",
                "sealed_lockbox_descriptor": sealed_lockbox_descriptor,
            },
        )
        lockbox_panel_fingerprints: dict[int, str] = {}
        lockbox_signals_by_horizon: dict[int, list[SignalEvent]] = {
            horizon: [] for horizon in development_eligible_horizons
        }
        lockbox_evaluation_timestamps_by_horizon: dict[int, list[object]] = {
            horizon: [] for horizon in development_eligible_horizons
        }
        lockbox_calibration_rows_by_horizon: dict[
            int, list[dict[str, object]]
        ] = {horizon: [] for horizon in development_eligible_horizons}
        lockbox_prediction_gates_by_horizon: dict[int, dict[str, object]] = {}
        lockbox_bybit_evidence_by_horizon: dict[int, dict[str, object]] = {}
        for horizon in development_eligible_horizons:
            lockbox_parts: list[pd.DataFrame] = []
            max_wait_sec = max(30, min(300, horizon // 2))
            timeframe = HORIZON_TIMEFRAME[horizon]
            lockbox_history: dict[
                str, tuple[pd.DataFrame, Sequence[MarketBar]]
            ] = {}
            for symbol in SYMBOLS:
                frame = self.source.load(
                    symbol, timeframe, self.config.max_bars_per_symbol
                )
                enriched = _engineer_features(frame)
                lockbox_history[symbol] = (enriched, _market_bars(enriched))
            last_complete_by_symbol = {
                symbol: lockbox_history[symbol][0]["close_at"]
                .max()
                .to_pydatetime()
                - timedelta(seconds=horizon + max_wait_sec)
                for symbol in SYMBOLS
            }
            lockbox_bybit_history: pd.DataFrame | None = None
            lockbox_bybit_evidence: dict[str, object] | None = None
            if lockbox_bybit_source is not None:
                requested_bybit_names = _bybit_names_for_horizon(
                    horizon, bybit_names
                )
                lockbox_bybit_history, lockbox_bybit_evidence = (
                    lockbox_bybit_source.load(
                        requested_bybit_names,
                        maximum_sequence=lockbox_bybit_maximum_sequence,
                        maximum_invalidation_rowid=(
                            lockbox_bybit_maximum_invalidation_rowid
                        ),
                        maximum_capture_audit_rowid=(
                            lockbox_bybit_maximum_capture_audit_rowid
                        ),
                        maximum_pit_import_rowid=(
                            lockbox_bybit_maximum_import_rowid
                        ),
                        minimum_decision_at=lockbox_start_by_horizon[horizon],
                        maximum_decision_at=max(last_complete_by_symbol.values()),
                        symbols=SYMBOLS,
                    )
                )
                lockbox_bybit_evidence_by_horizon[horizon] = (
                    lockbox_bybit_evidence
                )
            for symbol in SYMBOLS:
                enriched, bars = lockbox_history[symbol]
                if (
                    lockbox_bybit_history is not None
                    and lockbox_bybit_evidence is not None
                    and lockbox_bybit_source is not None
                ):
                    symbol_history = (
                        lockbox_bybit_history[
                            lockbox_bybit_history["symbol"].astype(str).str.upper()
                            == symbol
                        ].copy()
                        if not lockbox_bybit_history.empty
                        else lockbox_bybit_history.copy()
                    )
                    bars, _ = enrich_market_bars_with_bybit_execution_pit(
                        bars,
                        source=lockbox_bybit_source,
                        history=symbol_history,
                        source_evidence=lockbox_bybit_evidence,
                    )
                    del symbol_history
                # The final backtest must use the full immutable history that
                # contains this lockbox path.  Leaving the development-only
                # sequence here would reject every lockbox signal as missing.
                market[f"{symbol}:{horizon}"] = bars
                lockbox_parts.append(
                    _panel_frame(
                        enriched,
                        horizon,
                        bars,
                        decision_at_or_after=lockbox_start_by_horizon[horizon],
                        decision_before=last_complete_by_symbol[symbol],
                    )
                )
            lockbox_panel = pd.concat(lockbox_parts, ignore_index=True)
            lockbox_parts.clear()
            if trad_source is not None and trad_history is not None:
                lockbox_panel = trad_source.join(
                    lockbox_panel, history=trad_history
                )
            if macro_source is not None and macro_history is not None:
                lockbox_panel = macro_source.join(
                    lockbox_panel,
                    names=tuple(MACRO_FEATURE_CONTRACTS),
                    history=macro_history,
                )
            if flow_source is not None and flow_history is not None:
                lockbox_panel = flow_source.join(
                    lockbox_panel,
                    names=tuple(FLOW_FEATURE_CONTRACTS),
                    history=flow_history,
                )
            if (
                horizon in {180, 900}
                and lockbox_bybit_source is not None
                and lockbox_bybit_history is not None
            ):
                lockbox_panel = lockbox_bybit_source.join(
                    lockbox_panel,
                    names=bybit_names,
                    history=lockbox_bybit_history,
                )
            raw_lockbox_rows = len(lockbox_panel)
            if "execution_window_evidence_complete" in lockbox_panel.columns:
                direct_lockbox_mask = lockbox_panel[
                    "execution_window_evidence_complete"
                ].fillna(False).astype(bool)
                lockbox_panel = lockbox_panel.loc[
                    direct_lockbox_mask
                ].reset_index(drop=True)
            else:
                lockbox_panel = lockbox_panel.iloc[0:0].copy()
            if lockbox_panel.empty or lockbox_panel["symbol"].nunique() < 2:
                lockbox_panel_fingerprints[horizon] = _hash_payload(
                    {
                        "horizon_sec": horizon,
                        "status": "NO_DIRECT_EXECUTION_LOCKBOX_PANEL",
                        "raw_rows": raw_lockbox_rows,
                        "direct_rows": len(lockbox_panel),
                    }
                )
                self.ledger.append_event(
                    self.trial_id,
                    "running",
                    {
                        "phase": "lockbox_horizon_scored",
                        "horizon_sec": horizon,
                        "raw_panel_rows": raw_lockbox_rows,
                        "panel_rows": len(lockbox_panel),
                        "signals": 0,
                        "status": "FAILED_NO_DIRECT_EXECUTION_EVIDENCE",
                        "panel_fingerprint": lockbox_panel_fingerprints[horizon],
                    },
                )
                lockbox_history.clear()
                lockbox_bybit_history = None
                del lockbox_panel
                continue
            lockbox_panel = PooledPanelBuilder.validate(
                lockbox_panel, horizon
            )
            lockbox_evaluation_timestamps_by_horizon[horizon].extend(
                lockbox_panel["decision_at"].tolist()
            )
            lockbox_panel_fingerprints[horizon] = PooledPanelBuilder.fingerprint(
                lockbox_panel
            )
            lockbox_predictions = final_models[horizon].predict(lockbox_panel)
            lockbox_prediction_gates_by_horizon[horizon] = (
                prediction_gate_diagnostics(
                    lockbox_panel,
                    lockbox_predictions,
                    meta_threshold=final_models[
                        horizon
                    ].config.meta_trade_probability,
                )
            )
            horizon_signals = _signals_from_predictions(
                lockbox_panel, lockbox_predictions, horizon
            )
            lockbox_calibration_rows_by_horizon[horizon].extend(
                directional_calibration_rows(lockbox_panel, lockbox_predictions)
            )
            lockbox_signals_by_horizon[horizon].extend(horizon_signals)
            self.ledger.append_event(
                self.trial_id,
                "running",
                {
                    "phase": "lockbox_horizon_scored",
                    "horizon_sec": horizon,
                    "raw_panel_rows": raw_lockbox_rows,
                    "panel_rows": len(lockbox_panel),
                    "signals": len(horizon_signals),
                    "panel_fingerprint": lockbox_panel_fingerprints[horizon],
                },
            )
            lockbox_history.clear()
            lockbox_parts.clear()
            lockbox_bybit_history = None
            del lockbox_panel
        post_lockbox_kline_identity = _stable_file_identity(
            self.config.feature_store_path
        )
        if post_lockbox_kline_identity != lockbox_kline_identity:
            raise RuntimeError(
                "kline feature store changed while lockbox paths were evaluated"
            )
        lockbox_fingerprint = _hash_payload(
            {
                str(horizon): fingerprint
                for horizon, fingerprint in lockbox_panel_fingerprints.items()
            }
        )

        lockbox_calibration_by_horizon = {
            horizon: evaluate_quantile_coverage(
                lockbox_calibration_rows_by_horizon[horizon],
                required_horizons=[horizon],
            )
            for horizon in development_eligible_horizons
        }
        lockbox_calibration_evidence = evaluate_quantile_coverage(
            [
                row
                for horizon in development_eligible_horizons
                for row in lockbox_calibration_rows_by_horizon[horizon]
            ],
            required_horizons=development_eligible_horizons,
        )

        horizon_lockbox_gates: dict[int, ProfitabilityGateResult] = {}
        horizon_lockbox_reports: dict[int, dict[str, object]] = {}
        horizon_lockbox_statistical_evidence: dict[int, dict[str, object]] = {}
        for horizon in development_eligible_horizons:
            horizon_signals = lockbox_signals_by_horizon[horizon]
            horizon_report = backtest.run(horizon_signals, market)
            horizon_stress = backtest.run(
                horizon_signals, market, cost_multiplier=2.0
            )
            horizon_execution_evidence = _execution_release_evidence(
                horizon_report
            )
            horizon_statistical_evidence = final_evaluation_statistical_evidence(
                horizon_report,
                lockbox_evaluation_timestamps_by_horizon[horizon],
                number_of_trials=int(statistical_trial_audit["number_of_trials"]),
                frozen_development_evidence=(
                    horizon_development_statistical_evidence[horizon]
                ),
            )
            horizon_lockbox_statistical_evidence[horizon] = (
                horizon_statistical_evidence
            )
            horizon_gate = evaluate_profitability_gate(
                horizon_report.trades,
                [
                    fold
                    for fold in walk_forward
                    if int(fold["horizon_sec"]) == horizon
                ],
                initial_equity_usdt=horizon_report.initial_equity_usdt,
                two_x_cost_net_return=horizon_stress.net_return,
                mark_to_market_max_drawdown=horizon_report.max_drawdown,
                mark_to_market_evidence_complete=horizon_report.mark_to_market_used,
                execution_evidence_complete=bool(
                    horizon_execution_evidence[
                        "candidate_backtest_execution_evidence_complete"
                    ]
                ),
                factor_ablation_complete=bool(
                    factor_report["all_required_groups_evaluated"]
                ),
                statistical_overfit_evidence=horizon_statistical_evidence,
                calibration_coverage_evidence=(
                    lockbox_calibration_by_horizon[horizon]
                ),
                gate_scope="horizon",
                thresholds=ProfitabilityThresholds(),
            )
            horizon_lockbox_gates[horizon] = horizon_gate
            horizon_lockbox_reports[horizon] = {
                "gate": horizon_gate.to_dict(),
                "normal_cost": horizon_report.to_dict(include_trades=True),
                "two_x_cost": horizon_stress.to_dict(include_trades=False),
                "statistical_overfit_evidence": horizon_statistical_evidence,
            }
            self.ledger.append_event(
                self.trial_id,
                "running",
                {
                    "phase": "lockbox_horizon_gate_scored",
                    "horizon_sec": horizon,
                    "profitability_gate": horizon_gate.profitability_gate,
                    "blockers": list(horizon_gate.blockers),
                    "deflated_sharpe_probability": horizon_statistical_evidence.get(
                        "deflated_sharpe_probability"
                    ),
                    "frozen_development_cscv_pbo": horizon_statistical_evidence.get(
                        "probability_of_backtest_overfitting"
                    ),
                },
            )
        lockbox_signals = [
            signal
            for horizon in development_eligible_horizons
            for signal in lockbox_signals_by_horizon[horizon]
        ]
        lockbox_report = backtest.run(lockbox_signals, market)
        stressed_report = backtest.run(lockbox_signals, market, cost_multiplier=2.0)
        lockbox_signal_funnel_inputs = [
            {
                "horizon_sec": horizon,
                "fold_id": "single_use_lockbox",
                "prediction_gate": lockbox_prediction_gates_by_horizon.get(
                    horizon, {}
                ),
                "signals": len(lockbox_signals_by_horizon[horizon]),
                "trades": len(
                    horizon_lockbox_reports.get(horizon, {})
                    .get("normal_cost", {})
                    .get("trades", [])
                ),
            }
            for horizon in development_eligible_horizons
        ]
        lockbox_signal_funnel_evidence = signal_funnel_evidence(
            lockbox_signal_funnel_inputs,
            lockbox_report,
            scope="single_use_lockbox",
        )
        lockbox_intratrade_drawdown_evidence = intratrade_drawdown_evidence(
            lockbox_report,
            scope="single_use_lockbox",
        )
        lockbox_execution_evidence = _execution_release_evidence(lockbox_report)
        lockbox_candidate_execution_evidence_complete = bool(
            lockbox_execution_evidence[
                "candidate_backtest_execution_evidence_complete"
            ]
        )
        lockbox_statistical_evidence = final_evaluation_statistical_evidence(
            lockbox_report,
            [
                value
                for horizon in development_eligible_horizons
                for value in lockbox_evaluation_timestamps_by_horizon[horizon]
            ],
            number_of_trials=int(statistical_trial_audit["number_of_trials"]),
            frozen_development_evidence=development_statistical_evidence,
        )
        gate = evaluate_profitability_gate(
            lockbox_report.trades,
            eligible_walk_forward,
            initial_equity_usdt=lockbox_report.initial_equity_usdt,
            two_x_cost_net_return=stressed_report.net_return,
            mark_to_market_max_drawdown=lockbox_report.max_drawdown,
            mark_to_market_evidence_complete=lockbox_report.mark_to_market_used,
            execution_evidence_complete=(
                lockbox_candidate_execution_evidence_complete
            ),
            factor_ablation_complete=bool(factor_report["all_required_groups_evaluated"]),
            statistical_overfit_evidence=lockbox_statistical_evidence,
            calibration_coverage_evidence=lockbox_calibration_evidence,
            thresholds=ProfitabilityThresholds(),
        )
        gate = _require_precommitted_horizon_gates(
            gate, horizon_lockbox_gates
        )
        if not gate.passed:
            _archive_candidate_manifest(output, self.trial_id)
        write_profitability_report(output / "profitability_report.json", gate)
        calibration_release_passed = bool(
            development_calibration_evidence.get("passed")
            and lockbox_calibration_evidence.get("passed")
            and all(
                evidence.get("passed")
                for evidence in development_calibration_by_horizon.values()
            )
            and all(
                evidence.get("passed")
                for evidence in lockbox_calibration_by_horizon.values()
            )
        )
        _atomic_json(
            output / "calibration_coverage_report.json",
            {
                "schema_version": "profitability-calibration-release.v1",
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "status": "PASSED" if calibration_release_passed else "FAILED",
                "complete": bool(
                    development_calibration_evidence.get("complete")
                    and lockbox_calibration_evidence.get("complete")
                ),
                "development": {
                    "portfolio": development_calibration_evidence,
                    "horizons": {
                        str(horizon): evidence
                        for horizon, evidence in development_calibration_by_horizon.items()
                    },
                },
                "lockbox": {
                    "portfolio": lockbox_calibration_evidence,
                    "horizons": {
                        str(horizon): evidence
                        for horizon, evidence in lockbox_calibration_by_horizon.items()
                    },
                    "used_for_calibration_or_tuning": False,
                    "alternative_models_scored": False,
                },
            },
        )
        signal_funnel_complete = bool(
            development_signal_funnel_evidence.get("status") == "PASSED"
            and lockbox_signal_funnel_evidence.get("status") == "PASSED"
        )
        _atomic_json(
            output / "signal_funnel_report.json",
            {
                "schema_version": "profitability-signal-funnel.v1",
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "status": "PASSED" if signal_funnel_complete else "FAILED",
                "complete": signal_funnel_complete,
                "development": development_signal_funnel_evidence,
                "lockbox": lockbox_signal_funnel_evidence,
            },
        )
        intratrade_drawdown_complete = bool(
            development_intratrade_drawdown_evidence.get("status") == "PASSED"
            and lockbox_intratrade_drawdown_evidence.get("status") == "PASSED"
        )
        _atomic_json(
            output / "intratrade_drawdown_report.json",
            {
                "schema_version": "profitability-intratrade-drawdown.v1",
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "status": "PASSED" if intratrade_drawdown_complete else "FAILED",
                "complete": intratrade_drawdown_complete,
                "development": development_intratrade_drawdown_evidence,
                "lockbox": lockbox_intratrade_drawdown_evidence,
            },
        )
        _atomic_json(
            output / "lockbox_report.json",
            {
                "trial_id": self.trial_id,
                "status": "EVALUATED_ONCE",
                "lockbox_labels_materialized": True,
                "sealed_lockbox_descriptor": sealed_lockbox_descriptor,
                "lockbox_claim_identity": lockbox_claim_identity,
                "lockbox_source_identity": lockbox_source_identity,
                "lockbox_fingerprint": lockbox_fingerprint,
                "used_for_parameter_selection": False,
                "source_evidence": source_evidence,
                "lockbox_bybit_source_evidence": {
                    str(horizon): evidence
                    for horizon, evidence in lockbox_bybit_evidence_by_horizon.items()
                },
                "execution_evidence": lockbox_execution_evidence,
                "statistical_overfit_evidence": lockbox_statistical_evidence,
                "development_eligible_horizons": list(
                    development_eligible_horizons
                ),
                "candidate_horizon_selection_source": (
                    "frozen_feature_development_evaluation_oos_before_lockbox"
                ),
                "horizon_results": {
                    str(horizon): report
                    for horizon, report in horizon_lockbox_reports.items()
                },
                "final_development_selection": final_selection,
                "result": lockbox_report.to_dict(include_trades=True),
            },
        )
        _atomic_json(
            output / "statistical_overfit_report.json",
            {
                "trial_id": self.trial_id,
                "code_commit": self.config.code_commit,
                "data_snapshot_fingerprint": _hash_payload(
                    {
                        "development": source_evidence,
                        "lockbox": lockbox_source_identity,
                    }
                ),
                "feature_schema_hash": _hash_payload(
                    model_feature_columns_by_horizon
                ),
                "statistical_policy_hash": _hash_payload(
                    {
                        "candidate_configs": [
                            asdict(config) for config in candidate_configs
                        ],
                        "trial_count_audit": statistical_trial_audit,
                        "dsr_minimum_probability": 0.95,
                        "cscv_maximum_pbo": 0.05,
                        "cscv_partitions": 8,
                    }
                ),
                "evaluation_scope": "development_and_single_use_lockbox",
                "development_eligible_horizons": list(
                    development_eligible_horizons
                ),
                "thresholds": {
                    "minimum_deflated_sharpe_probability": 0.95,
                    "maximum_cscv_probability_of_backtest_overfitting": 0.05,
                },
                "trial_count_audit": statistical_trial_audit,
                "development": {
                    "portfolio": development_statistical_evidence,
                    "horizons": {
                        str(horizon): evidence
                        for horizon, evidence in horizon_development_statistical_evidence.items()
                    },
                },
                "lockbox": {
                    "portfolio": lockbox_statistical_evidence,
                    "horizons": {
                        str(horizon): evidence
                        for horizon, evidence in horizon_lockbox_statistical_evidence.items()
                    },
                    "alternative_variants_scored_on_lockbox": False,
                },
                "sources": [
                    "https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf",
                    "https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf",
                ],
            },
        )
        _atomic_json(
            output / "execution_cost_report.json",
            {
                "evaluation_scope": "lockbox",
                "execution_evidence_complete": (
                    lockbox_candidate_execution_evidence_complete
                ),
                "candidate_backtest_execution_evidence_complete": (
                    lockbox_candidate_execution_evidence_complete
                ),
                "live_execution_evidence_complete": bool(
                    lockbox_execution_evidence[
                        "live_execution_evidence_complete"
                    ]
                ),
                "execution_evidence": lockbox_execution_evidence,
                "development_execution_evidence": execution_evidence,
                "normal_cost": lockbox_report.to_dict(include_trades=False),
                "two_x_cost": stressed_report.to_dict(include_trades=False),
                "limitations": [
                    *(
                        []
                        if lockbox_report.proxy_execution_cost_trade_count == 0
                        else [
                            "one or more lockbox trades still use OHLCV-derived execution cost proxies"
                        ]
                    ),
                    *(
                        []
                        if lockbox_bybit_evidence_by_horizon
                        else ["no independently sealed lockbox Bybit execution source was supplied"]
                    ),
                    "official historical public data is not realized own-order fill evidence",
                    "immutable OOS shadow/testnet receipts and queue/latency calibration are incomplete",
                    "candidate evidence never authorizes live execution; live remains separately fail-closed",
                ],
            },
        )
        candidate_model_paths = {
            key: value
            for key, value in model_paths.items()
            if int(key) in development_eligible_horizons
        }
        candidate_model_hashes = {
            key: value
            for key, value in model_sha256.items()
            if int(key) in development_eligible_horizons
        }
        replay_artifact_integrity = bool(
            candidate_model_paths
            and all(
                _sha256_file(model_dir / relative_path)
                == candidate_model_hashes[key]
                for key, relative_path in candidate_model_paths.items()
            )
        )
        gate = _require_candidate_evidence(
            gate,
            check_name="production_replay_artifact_integrity",
            passed_check=replay_artifact_integrity,
        )
        gate = _require_candidate_evidence(
            gate,
            check_name="bybit_same_venue_price_path",
            passed_check=bool(same_venue_price_evidence["passed"]),
        )
        write_profitability_report(output / "profitability_report.json", gate)
        final_model_paths = candidate_model_paths if gate.passed else model_paths
        final_model_hashes = candidate_model_hashes if gate.passed else model_sha256
        final_bundle_payload = {
                "schema_version": "profitability-model-bundle.v2",
                "trial_id": self.trial_id,
                "model_family": "profitability_two_stage",
                "kline_source": self.config.kline_source,
                "price_venue": self.config.kline_source,
                "release_stage": "candidate" if gate.passed else "rejected",
                "profitability_gate": gate.profitability_gate,
                "models": final_model_paths,
                "model_sha256": final_model_hashes,
                "formal_feature_columns": {
                    str(horizon): list(columns)
                    for horizon, columns in model_feature_columns_by_horizon.items()
                    if not gate.passed
                    or horizon in development_eligible_horizons
                },
                "retained_factor_groups": list(retained_groups),
                "approved_horizons": (
                    list(development_eligible_horizons) if gate.passed else []
                ),
                "development_eligible_horizons": list(
                    development_eligible_horizons
                ),
                "candidate_horizon_selection_source": (
                    "frozen_feature_development_evaluation_oos_before_lockbox"
                ),
                "lockbox_fingerprint": lockbox_fingerprint,
                "lockbox_consumed": True,
                "code_commit": self.config.code_commit,
        }
        _atomic_json(bundle_path, final_bundle_payload)
        production_replay_path = output / "production_replay_report.json"
        production_replay_payload = json.loads(
            production_replay_path.read_text(encoding="utf-8")
        )
        production_replay_payload["final_model_bundle_sha256"] = _sha256_file(
            bundle_path
        )
        production_replay_payload["final_bundle_models_match_replayed"] = bool(
            replay_artifact_integrity
            and all(
                production_replay_payload.get("replayed_model_sha256", {}).get(key)
                == value
                for key, value in final_model_hashes.items()
            )
        )
        if not production_replay_payload[
            "final_bundle_models_match_replayed"
        ]:
            production_replay_payload["status"] = "FAILED"
            production_replay_payload["passed"] = False
            production_replay_payload["complete"] = False
        _atomic_json(production_replay_path, production_replay_payload)
        if gate.passed:
            create_candidate_manifest(
                output / "candidate_release_manifest.json",
                gate=gate,
                profitability_report_path=output / "profitability_report.json",
                model_artifact_path=bundle_path,
                lockbox_fingerprint=lockbox_fingerprint,
                code_commit=self.config.code_commit,
                evidence_report_paths={
                    name: output / name
                    for name in (
                        "walk_forward_report.json",
                        "lockbox_report.json",
                        "factor_ablation_report.json",
                        "execution_cost_report.json",
                        "capital_preservation_report.json",
                        "statistical_overfit_report.json",
                        "data_coverage_report.json",
                        "missing_intervals_report.json",
                        "independent_timestamp_count_report.json",
                        "calibration_coverage_report.json",
                        "nested_cv_report.json",
                        "signal_funnel_report.json",
                        "intratrade_drawdown_report.json",
                        "production_replay_report.json",
                    )
                },
            )
        record = TrialRecord(
            trial_id=self.trial_id,
            model_family="profitability_two_stage",
            data_signature=_hash_payload(source_evidence)[:24],
            parameter_hash=TrialLedger.parameter_hash(
                {
                    "candidate_configs": [asdict(config) for config in candidate_configs],
                    "features_by_horizon": model_feature_columns_by_horizon,
                    "nested_walk_forward": True,
                    "walk_forward_folds": self.config.walk_forward_folds,
                    "development_stage_policy": (
                        "chronological_disjoint_factor_research_then_frozen_feature_evaluation"
                    ),
                }
            ),
            code_commit=self.config.code_commit,
            status="completed" if gate.passed else "rejected",
            metrics=gate.to_dict(),
        )
        self.ledger.append(record)
        self.ledger.append_event(
            self.trial_id, "completed" if gate.passed else "rejected", gate.to_dict()
        )
        return gate

    def record_failure(self, reason: str) -> None:
        """Persist an incomplete experiment without pretending it reached lockbox."""

        metrics = {
            "profitability_gate": "FAILED",
            "stage": "rejected",
            "candidate_count": 0,
            "live_count": 0,
            "pipeline_error": reason,
        }
        self.ledger.append_event(self.trial_id, "failed", metrics)
        record = TrialRecord(
            trial_id=self.trial_id,
            model_family="profitability_two_stage",
            data_signature="pipeline_incomplete",
            parameter_hash=TrialLedger.parameter_hash(
                {
                    "max_bars_per_symbol": self.config.max_bars_per_symbol,
                    "walk_forward_folds": self.config.walk_forward_folds,
                    "horizons": HORIZONS_SEC,
                    "symbols": SYMBOLS,
                }
            ),
            code_commit=self.config.code_commit,
            status="failed",
            metrics=metrics,
        )
        try:
            self.ledger.append(record)
        except ValueError:
            # An exact run may already have a terminal record.  The append-only
            # event above still preserves this failed invocation.
            pass


def write_failed_outputs(output_dir: Path, *, reason: str) -> ProfitabilityGateResult:
    _archive_candidate_manifest(output_dir, "pipeline_failed")
    result = ProfitabilityGateResult(
        profitability_gate="FAILED",
        stage="rejected",
        candidate_count=0,
        live_count=0,
        checks={"pipeline_completed": {"passed": False, "reason": reason}},
        metrics={"trade_count": 0, "net_return": None, "max_drawdown": None},
        blockers=("pipeline_completed",),
    )
    write_profitability_report(output_dir / "profitability_report.json", result)
    for name, payload in (
        ("walk_forward_report.json", {"status": "FAILED", "reason": reason, "folds": []}),
        ("lockbox_report.json", {"status": "FAILED", "reason": reason, "used_for_parameter_selection": False}),
        ("factor_ablation_report.json", _factor_ablation_report()),
        ("execution_cost_report.json", {"status": "FAILED", "reason": reason, "execution_evidence_complete": False}),
        ("capital_preservation_report.json", policy_report(CapitalPreservationConfig())),
        (
            "statistical_overfit_report.json",
            {
                "status": "FAILED",
                "reason": reason,
                "complete": False,
                "deflated_sharpe_probability": None,
                "probability_of_backtest_overfitting": None,
            },
        ),
        (
            "calibration_coverage_report.json",
            {
                "schema_version": "profitability-calibration-release.v1",
                "status": "FAILED",
                "complete": False,
                "reason": reason,
                "development": {"status": "INCOMPLETE"},
                "lockbox": {
                    "status": "SEALED_NOT_OPENED",
                    "used_for_calibration_or_tuning": False,
                    "alternative_models_scored": False,
                },
            },
        ),
        (
            "nested_cv_report.json",
            {
                "schema_version": "profitability-nested-cv.v1",
                "status": "FAILED",
                "complete": False,
                "reason": reason,
                "outer_oos_used_for_tuning": False,
                "folds": [],
            },
        ),
        (
            "signal_funnel_report.json",
            {
                "schema_version": "profitability-signal-funnel.v1",
                "status": "FAILED",
                "complete": False,
                "reason": reason,
                "development": {"status": "INCOMPLETE"},
                "lockbox": {"status": "SEALED_NOT_OPENED"},
            },
        ),
        (
            "intratrade_drawdown_report.json",
            {
                "schema_version": "profitability-intratrade-drawdown.v1",
                "status": "FAILED",
                "complete": False,
                "reason": reason,
                "development": {"status": "INCOMPLETE"},
                "lockbox": {"status": "SEALED_NOT_OPENED"},
            },
        ),
        (
            "production_replay_report.json",
            {
                "schema_version": "profitability-production-replay.v1",
                "status": "FAILED",
                "passed": False,
                "complete": False,
                "reason": reason,
                "lockbox_used": False,
                "alternative_models_scored": False,
                "expected_sample_count": len(HORIZONS_SEC) * len(SYMBOLS),
                "observed_sample_count": 0,
                "failed_sample_count": 0,
                "samples": [],
            },
        ),
    ):
        _atomic_json(output_dir / name, payload)
    for name, schema_version in (
        ("data_coverage_report.json", "profitability-data-coverage.v1"),
        ("missing_intervals_report.json", "profitability-missing-intervals.v1"),
        (
            "independent_timestamp_count_report.json",
            "profitability-independent-timestamps.v1",
        ),
    ):
        path = output_dir / name
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {
                "schema_version": schema_version,
                "status": "FAILED",
                "complete": False,
            }
        payload["pipeline_status"] = "FAILED"
        payload["pipeline_failure_reason"] = reason
        payload["release_eligible"] = False
        _atomic_json(path, payload)
    return result


__all__: Sequence[str] = (
    "ProfitabilityRebuild",
    "ProfitabilityRebuildConfig",
    "MINIMUM_COVERAGE_DAYS",
    "SYMBOLS",
    "audit_source_coverage",
    "validate_source_coverage",
    "write_failed_outputs",
)
