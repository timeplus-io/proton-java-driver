package com.clickhouse.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public enum ClickHouseAggregateFunction {
    // select concat(f.name, '(', f.case_insensitive ? 'true' : 'false',
    // a.names != '' ? concat(',', replaceAll(replaceRegexpAll(a.names,
    // '^\\[(.*)\\]$', '\\1'), '''', '"')) : '', '),') as x
    // from system.functions f
    // left outer join (
    // select alias_to, toString(groupArray(case_insensitive ? upper(name) : name))
    // as names
    // from system.functions
    // where is_aggregate != 0 and alias_to != ''
    // group by alias_to
    // ) a on f.name = a.alias_to
    // where f.is_aggregate != 0 and f.alias_to = ''
    // order by f.name
    agg_throw(false, 0, true, 0, ClickHouseDataType.nothing),
    any(false, 1, true, 1, null),
    any_heavy(false, 1, false, 1, null),
    any_last(false, 1, true, 1, null),
    arg_max(false, 2, false, 1, null),
    arg_min(false, 2, false, 1, null),
    avg(true, 1, false, 1, null),
    avg_weighted(false, 2, false, 1, null),
    bounding_ratio(false, 2, false, 0, ClickHouseDataType.float64),
    categorical_information_value(false, Integer.MAX_VALUE, false, 0, ClickHouseDataType.float64),
    corr(true, 2, false, 0, ClickHouseDataType.float64),
    corr_stable(false, 2, false, 0, ClickHouseDataType.float64),
    count(true, 1, true, 0, ClickHouseDataType.uint64),
    covar_pop(false, 2, false, 0, ClickHouseDataType.float64, "COVAR_POP"),
    covar_pop_stable(false, 2, false, 0, ClickHouseDataType.float64),
    covar_samp(false, 2, false, 0, ClickHouseDataType.float64, "COVAR_SAMP"),
    covar_samp_stable(false, 2, false, 0, ClickHouseDataType.float64),
    delta_sum(false, 1, false, 1, null),
    delta_sum_timestamp(false, 2, false, 1, null),
    dense_rank(true),
    entropy(false, 1, false, 0, ClickHouseDataType.float64),
    exponential_moving_average(false, 3, false, 0, ClickHouseDataType.float64),
    exponential_time_decayed_avg(false),
    exponential_time_decayed_count(false),
    exponential_time_decayed_max(false),
    exponential_time_decayed_sum(false),
    first_value(true, 1, true, 1, null),
    group_array(false, 2, false, 1, null),
    group_array_insert_at(false),
    group_array_moving_avg(false),
    group_array_moving_sum(false),
    group_array_sample(false),
    group_bit_and(false, "bit_and"),
    group_bit_or(false, "bit_or"),
    group_bit_xor(false, "bit_xor"),
    group_bitmap(false),
    group_bitmap_and(false),
    group_bitmap_or(false),
    group_bitmap_xor(false),
    group_uniq_array(false),
    histogram(false),
    interval_length_sum(false),
    kurt_pop(false),
    kurt_samp(false),
    lag_in_frame(false),
    last_value(true, 1, true, 1, null),
    leadInFrame(false),
    mannWhitneyUTest(false),
    max(true, 1, true, 1, null),
    maxIntersections(false),
    maxIntersectionsPosition(false),
    maxMappedArrays(false),
    min(true, 1, true, 1, null),
    minMappedArrays(false),
    quantile(false, 2, false, 0, ClickHouseDataType.float64, "median"),
    quantileBFloat16(false, 2, false, 0, ClickHouseDataType.float64,
            "medianBFloat16"),
    quantileBFloat16Weighted(false, "medianBFloat16Weighted"),
    quantileDeterministic(false, "medianDeterministic"),
    quantileExact(false, "medianExact"),
    quantileExactExclusive(false),
    quantileExactHigh(false, "medianExactHigh"),
    quantileExactInclusive(false),
    quantileExactLow(false, "medianExactLow"),
    quantileExactWeighted(false, "medianExactWeighted"),
    quantileTDigest(false, "medianTDigest"),
    quantileTDigestWeighted(false, "medianTDigestWeighted"),
    quantileTiming(false, "medianTiming"),
    quantileTimingWeighted(false, "medianTimingWeighted"),
    quantiles(false, Integer.MAX_VALUE, false, 0, ClickHouseDataType.float64),
    quantilesBFloat16(false),
    quantilesBFloat16Weighted(false),
    quantilesDeterministic(false),
    quantilesExact(false),
    quantilesExactExclusive(false),
    quantilesExactHigh(false),
    quantilesExactInclusive(false),
    quantilesExactLow(false),
    quantilesExactWeighted(false),
    quantilesTDigest(false),
    quantilesTDigestWeighted(false),
    quantilesTiming(false),
    quantilesTimingWeighted(false),
    rank(true),
    rankCorr(false),
    retention(false),
    row_number(true),
    sequence_count(false),
    sequence_match(false),
    sequence_next_node(false),
    simple_linear_regression(false),
    single_value_or_null(false),
    skew_pop(false),
    skew_samp(false),
    sparkbar(false),
    stddev_pop(false, "stddev_pop"),
    stddev_pop_stable(false),
    stddev_samp(false, "stddev_samp"),
    stddev_samp_stable(false),
    stochastic_linear_regression(false),
    stochastic_logistic_regression(false),
    studentTTest(false),
    sum(true, 1, true, 1, null),
    sum_count(false),
    sum_kahan(false),
    sum_map_filtered(false),
    sum_map_filtered_with_overflow(false),
    sum_map_with_overflow(false),
    sum_mapped_arrays(false),
    sum_with_overflow(false),
    top_k(false),
    top_k_weighted(false),
    uniq(false),
    uniq_combined(false),
    uniq_combined64(false),
    uniq_exact(false),
    uniq_hll12(false),
    uniq_theta(false),
    uniq_up_to(false),
    var_pop(false, "var_pop"),
    var_pop_stable(false),
    var_samp(false, "var_samp"),
    var_samp_stable(false),
    welchTTest(false),
    window_funnel(false);

    public static final Map<String, ClickHouseAggregateFunction> name2func;

    static {
        Map<String, ClickHouseAggregateFunction> map = new HashMap<>();
        String errorMsg = "[%s] is used by type [%s]";
        ClickHouseAggregateFunction used = null;
        for (ClickHouseAggregateFunction t : ClickHouseAggregateFunction.values()) {
            String name = t.name();
            if (!t.isCaseSensitive()) {
                name = name.toUpperCase();
            }
            used = map.put(name, t);
            if (used != null) {
                throw new IllegalStateException(String.format(Locale.ROOT, errorMsg, name, used.name()));
            }
        }

        name2func = Collections.unmodifiableMap(map);
    }

    /**
     * Converts given type name to corresponding aggregate function.
     *
     * @param function non-empty function
     * @return aggregate function
     */
    public static ClickHouseAggregateFunction of(String function) {
        if (function == null || (function = function.trim()).isEmpty()) {
            throw new IllegalArgumentException("Non-empty function is required");
        }

        ClickHouseAggregateFunction f = name2func.get(function);
        if (f == null) {
            f = name2func.get(function.toUpperCase()); // case-insensitive or just an alias
        }

        if (f == null) {
            throw new IllegalArgumentException("Unknown aggregate function: " + function);
        }
        return f;
    }

    private final boolean caseSensitive;
    private final int maxArgs;
    private final boolean singleValue;
    private final int refArgIndex;
    private final ClickHouseDataType valueType;
    private final List<String> aliases;

    ClickHouseAggregateFunction(boolean caseSensitive, String... aliases) {
        this(caseSensitive, 1, false, 0, ClickHouseDataType.nothing, aliases);
    }

    ClickHouseAggregateFunction(boolean caseSensitive, int maxArgs, boolean singleValue, int refArgIndex,
            ClickHouseDataType valueType, String... aliases) {
        this.caseSensitive = caseSensitive;
        this.maxArgs = maxArgs < 0 ? 0 : maxArgs;
        this.singleValue = singleValue;
        this.refArgIndex = refArgIndex < 0 ? 0 : (refArgIndex > this.maxArgs ? this.maxArgs : refArgIndex);
        this.valueType = this.refArgIndex > 0 || valueType == null ? ClickHouseDataType.nothing : valueType;
        if (aliases == null || aliases.length == 0) {
            this.aliases = Collections.emptyList();
        } else {
            this.aliases = Collections.unmodifiableList(Arrays.asList(aliases));
        }
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public List<String> getAliases() {
        return aliases;
    }
}
