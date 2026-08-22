import logging
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / 'data'
log = logging.getLogger("SupportStrategy")  # 为此模块创建 logger 实例


def find_support_points(data, min_threshold=0.2):
    liq_map = data['liqMapV2']
    last_price = float(data['lastPrice'])

    price_levels = sorted([float(p) for p in liq_map.keys()])
    if not price_levels:
        log.warning("No price levels found in liq_map.")  # 增加日志
        return {'long': [], 'short': []}

    current_index = min(range(len(price_levels)), key=lambda i: abs(price_levels[i] - last_price))

    def get_volume(price):
        # 自动匹配最接近价格，防止 key 精度问题
        nearest_price_key = min(liq_map.keys(), key=lambda p_key: abs(float(p_key) - price))
        entries = liq_map[nearest_price_key]
        return sum([entry[1] for entry in entries])

    def find_top_points(prices):
        points = []
        for price in prices:
            volume = get_volume(price)
            if volume >= min_threshold:
                points.append({'price': price, 'volume': volume})
        points.sort(key=lambda x: x['volume'], reverse=True)  # 按量能降序排序
        return points

    lower_prices = price_levels[:current_index][::-1]  # 向下找 (价格从当前向低排序)
    upper_prices = price_levels[current_index + 1:]  # 向上找 (价格从当前向高排序)

    down_points = find_top_points(lower_prices)
    up_points = find_top_points(upper_prices)

    # --- 优化后的 select_points 函数 START ---
    def select_points(point_candidates, direction='down'):
        if not point_candidates:
            log.warning(f"No point candidates for direction: {direction}. Using default price: {last_price}")
            # 如果没有候选点，提供基于最新价格的备用点，确保不同且有意义
            if direction == 'down':
                return [
                    {'price': last_price, 'label': '默认强支撑'},
                    {'price': last_price * 0.99, 'label': '默认次支撑'},  # 略低于最新价
                    {'price': last_price * 0.98, 'label': '默认保底支撑'}  # 再次低于最新价
                ]
            else:  # 'up'
                return [
                    {'price': last_price, 'label': '默认强阻力'},
                    {'price': last_price * 1.01, 'label': '默认次阻力'},  # 略高于最新价
                    {'price': last_price * 1.02, 'label': '默认保底阻力'}  # 再次高于最新价
                ]

        # 优先按量能降序排序候选点
        sorted_candidates = sorted(point_candidates, key=lambda x: x['volume'], reverse=True)

        selected_points_list = []

        # 1. 选择第一点 (量能最大的点)
        first_point = sorted_candidates[0]
        selected_points_list.append(first_point)
        log.debug(
            f"Direction {direction}: First point selected: {first_point['price']:.2f} (Vol: {first_point['volume']:.2f})")

        # 准备剩余候选点，排除已选的第一点
        remaining_after_first = [p for p in sorted_candidates if p != first_point]

        # 2. 选择第二点 (次级支撑/阻力)
        second_point_found = False
        for p in remaining_after_first:
            if direction == 'down':  # 寻找支撑：价格必须严格低于第一点
                if p['price'] < first_point['price']:
                    selected_points_list.append(p)
                    second_point_found = True
                    log.debug(
                        f"Direction {direction}: Second point selected: {p['price']:.2f} (Vol: {p['volume']:.2f})")
                    break
            else:  # 寻找阻力：价格必须严格高于第一点
                if p['price'] > first_point['price']:
                    selected_points_list.append(p)
                    second_point_found = True
                    log.debug(
                        f"Direction {direction}: Second point selected: {p['price']:.2f} (Vol: {p['volume']:.2f})")
                    break

        if not second_point_found:
            # 如果没有找到合适的第二点，使用备用价格（基于第一点略微偏移）
            fallback_price = first_point['price'] * (0.99 if direction == 'down' else 1.01)
            selected_points_list.append(
                {'price': fallback_price, 'label': f'保底次{"支撑" if direction == "down" else "阻力"}'})
            log.warning(f"Direction {direction}: No suitable second point found. Using fallback: {fallback_price:.2f}")

        # 准备剩余候选点，排除已选的第二点
        current_second_point = selected_points_list[1]  # 获取实际的第二点 (可能是备用点)
        remaining_after_second = [p for p in remaining_after_first if p != current_second_point]

        # 3. 选择第三点 (保底支撑/阻力)
        third_point_found = False
        for p in remaining_after_second:
            if direction == 'down':  # 寻找支撑：价格必须严格低于第二点
                if p['price'] < current_second_point['price']:
                    selected_points_list.append(p)
                    third_point_found = True
                    log.debug(f"Direction {direction}: Third point selected: {p['price']:.2f} (Vol: {p['volume']:.2f})")
                    break
            else:  # 寻找阻力：价格必须严格高于第二点
                if p['price'] > current_second_point['price']:
                    selected_points_list.append(p)
                    third_point_found = True
                    log.debug(f"Direction {direction}: Third point selected: {p['price']:.2f} (Vol: {p['volume']:.2f})")
                    break

        if not third_point_found:
            # 如果没有找到合适的第三点，使用备用价格（基于第二点略微偏移）
            fallback_price = current_second_point['price'] * (0.98 if direction == 'down' else 1.02)
            selected_points_list.append(
                {'price': fallback_price, 'label': f'保底{"支撑" if direction == "down" else "阻力"}'})  # 调整标签
            log.warning(f"Direction {direction}: No suitable third point found. Using fallback: {fallback_price:.2f}")

        # 最终为选出的点分配标签
        selected_points_list[0]['label'] = '强支撑' if direction == 'down' else '强阻力'
        selected_points_list[1]['label'] = '次支撑' if direction == 'down' else '次阻力'
        selected_points_list[2]['label'] = '保底支撑' if direction == 'down' else '保底阻力'

        return selected_points_list

    # --- 优化后的 select_points 函数 END ---

    selected_down = select_points(down_points, 'down')
    selected_up = select_points(up_points, 'up')

    # 这里的 long_points 和 short_points 结构保持不变，因为 select_points 已经返回了处理好的三个点
    long_points = [
        {'price': selected_down[0]['price'], 'label': selected_down[0]['label']},
        {'price': selected_down[1]['price'], 'label': selected_down[1]['label']},
        {'price': selected_down[2]['price'], 'label': selected_down[2]['label']}
    ]

    short_points = [
        {'price': selected_up[0]['price'], 'label': selected_up[0]['label']},
        {'price': selected_up[1]['price'], 'label': selected_up[1]['label']},
        {'price': selected_up[2]['price'], 'label': selected_up[2]['label']}
    ]

    return {'long': long_points, 'short': short_points}