"""
Step 3: 对 photo/ 目录下的地点图片调用 VLM 标注，写入 label_platform.db

10 个图片地点（无上传视频，仅有代表图）：
  十堰 2 个 | 抚州 3 个 | 贵港 3 个 | 安阳 2 个

运行：
  conda activate nds
  python step3_photo_label.py
"""

import base64
import io
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import anthropic
from PIL import Image

os.environ.setdefault("ANTHROPIC_API_KEY", "sk-3iebUvSCeu1TO0F5JYJl23BE0U0VKBnLOjShTEfI8ZM34DMb")

PHOTO_DIR   = Path("/home/stu6/projects/LabelWork1/photo")
DB_FILE     = "/home/stu6/projects/LabelWork1/auto_labeled.db"
CACHE_FILE  = "/home/stu6/projects/LabelWork1/photo_vlm_cache.json"
MODEL       = "claude-haiku-4-5-20251001"
MAX_SIDE    = 1024   # 图片最长边（图片地点建议稍大以看清路面细节）

# ─── 地点元数据 ──────────────────────────────────────────────────────────
# 字段说明：
#   photo        : photo/ 目录下的文件名
#   city         : 城市（中文）
#   scene_type   : 场景类型（来自任务表格）
#   location_name: 地点名称
#   flight_height: 飞行高度（米）
#   data_size_gb : 最终数据总量（GB）
PHOTO_LOCATIONS = [
    {
        "photo": "shiyan1.PNG",
        "city": "十堰",
        "scene_type": "环岛+高速路出入口",
        "location_name": "麻安高速",
        "flight_height": 200,
        "data_size_gb": 20.42,
    },
    {
        "photo": "shiyan2.png",
        "city": "十堰",
        "scene_type": "T型路口",
        "location_name": "园林路",
        "flight_height": 100,
        "data_size_gb": 16.62,
    },
    {
        "photo": "fuzhou1.PNG",
        "city": "抚州",
        "scene_type": "T型路口",
        "location_name": "旧小区",
        "flight_height": 100,
        "data_size_gb": 30.0,
    },
    {
        "photo": "fuzhou2.PNG",
        "city": "抚州",
        "scene_type": "十字路口",
        "location_name": "龙山大道",
        "flight_height": 120,
        "data_size_gb": 25.0,
    },
    {
        "photo": "fuzhou3.PNG",
        "city": "抚州",
        "scene_type": "十字路口",
        "location_name": "街心花园",
        "flight_height": 100,
        "data_size_gb": 25.0,
    },
    {
        "photo": "guigang1.PNG",
        "city": "贵港",
        "scene_type": "拥挤路段",
        "location_name": "金港大道",
        "flight_height": 120,
        "data_size_gb": 10.0,
    },
    {
        "photo": "guigang2.PNG",
        "city": "贵港",
        "scene_type": "立交桥",
        "location_name": "北环金港立交",
        "flight_height": 300,
        "data_size_gb": 10.0,
    },
    {
        "photo": "guigang3.jpg",
        "city": "贵港",
        "scene_type": "工业区支路",
        "location_name": "工园支路",
        "flight_height": 120,
        "data_size_gb": 10.0,
    },
    {
        "photo": "guigang4.jpg",
        "city": "贵港",
        "scene_type": "合流区",
        "location_name": "北环金港匝道",
        "flight_height": 120,
        "data_size_gb": 30.0,
    },
    {
        "photo": "anyang1.png",
        "city": "安阳",
        "scene_type": "拥挤路段",
        "location_name": "集市路",
        "flight_height": 100,
        "data_size_gb": 4.0,
    },
    {
        "photo": "anyang2.jpg",
        "city": "安阳",
        "scene_type": "高速路",
        "location_name": "台辉高速",
        "flight_height": 120,
        "data_size_gb": 0.45,
    },
]

# ─── 标签体系（与其他 step 保持一致）────────────────────────────────────
TOP_LEVEL_CONFIG = {
    "区域": ["封闭园区", "交通管制区域", "开放道路"],
    "城市道路": ["快速路", "主干路", "次干路", "支路", "街巷"],
    "公路": ["高速公路", "一级公路", "二级公路", "三级公路", "四级公路"],
    "乡村道路": ["村道", "其他乡村内部道路"],
    "其他道路": ["厂矿", "林区", "港口", "专用道路"],
    "停车区域": ["室内停车场", "室外停车场", "路侧停车位"],
    "自动驾驶场景": ["封闭场景", "半封闭场景", "开放场景"],
}

LABEL_SCHEMA = {
    "一、道路静态环境": {
        "1.2 道路表面": {
            "表面类型": ["沥青", "混凝土", "土路", "碎石", "冰雪路面", "金属板"],
            "表面状态": ["干燥", "潮湿", "积水", "积雪", "结冰", "泥泞"],
        },
        "1.3 道路几何": {
            "坡度": ["平路", "上坡", "下坡", "起伏路"],
            "曲率": ["直线", "弯道 (曲率<0.01)", "弯道 (0.01<曲率<0.05)", "弯道 (曲率>0.05)"],
            "横坡": ["正常排水坡度", "反超高", "无横坡"],
        },
        "1.4 包含车道特征": {
            "最宽车道数量": ["单车道", "双车道", "三车道", "四车道及以上"],
            "车道类型": ["普通车道", "公交专用道", "HOV车道", "潮汐车道", "应急车道",
                        "非机动车道", "人行道", "汇入匝道", "汇出匝道"],
            "车道宽度": ["标准", "狭窄", "超宽"],
        },
        "1.5 道路边缘": {
            "边缘类型": ["路缘石", "护栏 (金属)", "护栏 (混凝土)", "草地/泥土", "无物理隔离"],
        },
        "1.6 道路交叉": {
            "交叉类型": ["路段 (无交叉)", "平面交叉 (十字)", "平面交叉 (丁字)",
                        "平面交叉 (畸形)", "大型环岛 (出入口数 > 4)", "小型环岛", "立体交叉"],
        },
    },
    "二、交通设施": {
        "2.1 交通控制": {
            "信号灯": ["有", "无"],
            "标志牌": ["限速", "禁止", "指示", "警告", "施工", "无"],
            "地面标签": ["实线", "虚线", "双黄线", "导流线", "斑马线", "标线磨损"],
        },
        "2.2 路侧与周边环境": {
            "设施": ["无", "路灯", "电线杆", "隔音墙", "路边树木", "路边停车位",
                    "地面停车场出入口", "隧道出入口", "居民楼", "商场", "学校",
                    "医院", "公园", "绿化带"],
        },
        "2.3 特殊设施": {
            "类型": ["收费站", "检查站", "施工区域围挡", "减速带", "无"],
        },
    },
    "三、动态目标 (路面状况)": {
        "3.1 机动车": {
            "类型": ["轿车", "客车/巴士", "卡车/货车", "特种车辆 (警)",
                    "特种车辆(消)", "特种车辆(救)", "工程车辆"],
        },
        "3.2 VRU": {
            "类型": ["自行车", "电动车", "三轮车", "行人", "无"],
        },
        "3.3 动物": {"类型": ["有", "无"]},
        "3.4 障碍物": {
            "类型": ["落石", "遗洒物", "倒伏树木", "锥桶", "无"],
        },
        "3.5 事故车辆": {"类型": ["有", "无"]},
    },
    "四、大气环境": {
        "4.1 天气": {
            "类型": ["晴", "多云", "阴", "雨 (小/中/大)", "雪", "雾", "冰雹"],
        },
        "4.2 颗粒物": {
            "类型": ["无", "雾霾", "沙尘", "烟尘"],
        },
        "4.3 光照": {
            "来源": ["自然光", "人工照明", "混合光"],
            "强度": ["正常", "强光/逆光", "弱光/昏暗", "黑暗"],
        },
        "4.4 气温": {
            "估算": ["极寒 (< -20℃)", "寒冷 (-20℃ ~ -10℃)", "舒适 (-10℃ ~ 10℃)",
                    "炎热 (10℃ ~ 20℃)", "极热 (> 20℃)"],
        },
    },
}


# ─── 图片处理 ────────────────────────────────────────────────────────────

def resize_image(image_path: str) -> tuple[bytes, str]:
    with Image.open(image_path) as img:
        w, h = img.size
        if max(w, h) > MAX_SIDE:
            scale = MAX_SIDE / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=88)
        return buf.getvalue(), "image/jpeg"


# ─── VLM 调用 ────────────────────────────────────────────────────────────

def build_prompt(scene_type: str, city: str, location_name: str, flight_height: int) -> str:
    schema_str = json.dumps(LABEL_SCHEMA, ensure_ascii=False, indent=2)
    top_str    = json.dumps(TOP_LEVEL_CONFIG, ensure_ascii=False, indent=2)
    return f"""你是一名自动驾驶数据标注专家。这是一张无人机航拍视角的道路图像。
拍摄地点：{city} · {location_name}（场景类型：{scene_type}，飞行高度：{flight_height}m）

请根据图像内容，按照以下标签体系输出 JSON 格式的 ODD（运行设计域）标注结果。

【顶层道路类型】（从下列选择一个 category 和一个 subcategory）：
{top_str}

【次级标签体系】（每个字段从给定选项中选择；标注了多选的字段返回列表，其余返回单个字符串）：
{schema_str}

多选字段（返回列表）：曲率、车道类型、车道宽度、地面标签、设施、3.1机动车类型、3.2VRU类型、3.4障碍物类型、交叉类型

注意事项：
1. 只从给定选项中选择，不要自造新标签
2. 无法判断的字段，选最接近的选项
3. 从航拍视角推断道路几何特征
4. 气温根据植被、积雪、服装等视觉线索推断

请严格按如下 JSON 结构输出，不要有多余文字：
{{
  "top_road_category": "城市道路",
  "top_road_subcategory": "主干路",
  "tags": {{
    "一、道路静态环境": {{
      "1.2 道路表面": {{"表面类型": "沥青", "表面状态": "干燥"}},
      "1.3 道路几何": {{"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"}},
      "1.4 包含车道特征": {{"最宽车道数量": "四车道及以上", "车道类型": ["普通车道"], "车道宽度": ["标准"]}},
      "1.5 道路边缘": {{"边缘类型": "路缘石"}},
      "1.6 道路交叉": {{"交叉类型": ["路段 (无交叉)"]}}
    }},
    "二、交通设施": {{
      "2.1 交通控制": {{"信号灯": "有", "标志牌": "无", "地面标签": ["实线"]}},
      "2.2 路侧与周边环境": {{"设施": ["路灯"]}},
      "2.3 特殊设施": {{"类型": "无"}}
    }},
    "三、动态目标 (路面状况)": {{
      "3.1 机动车": {{"类型": ["轿车"]}},
      "3.2 VRU": {{"类型": ["无"]}},
      "3.3 动物": {{"类型": "无"}},
      "3.4 障碍物": {{"类型": ["无"]}},
      "3.5 事故车辆": {{"类型": "无"}}
    }},
    "四、大气环境": {{
      "4.1 天气": {{"类型": "晴"}},
      "4.2 颗粒物": {{"类型": "无"}},
      "4.3 光照": {{"来源": "自然光", "强度": "正常"}},
      "4.4 气温": {{"估算": "舒适 (-10℃ ~ 10℃)"}}
    }}
  }}
}}"""


def call_vlm(client: anthropic.Anthropic, loc: dict) -> dict | None:
    image_path = PHOTO_DIR / loc["photo"]
    if not image_path.exists():
        print(f"  [WARN] 图片不存在: {image_path}")
        return None

    raw_bytes, media_type = resize_image(str(image_path))
    prompt = build_prompt(
        loc["scene_type"], loc["city"], loc["location_name"], loc["flight_height"]
    )

    content = [
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.standard_b64encode(raw_bytes).decode("utf-8"),
            },
        },
        {"type": "text", "text": prompt},
    ]

    for attempt in range(6):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=2500,
                messages=[{"role": "user", "content": content}],
            )
            text_block = next((b for b in response.content if b.type == "text"), None)
            text = text_block.text.strip() if text_block else ""
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                result = json.loads(m.group())
                if "top_road_category" in result and "tags" in result:
                    return result
                print(f"  [WARN] 响应结构不完整，重试 ({attempt+1})")
            else:
                print(f"  [WARN] 响应无 JSON ({attempt+1}): {text[:200]!r}")
            time.sleep(3)
        except json.JSONDecodeError as e:
            print(f"  [WARN] JSON 解析失败 ({attempt+1}): {e}")
            time.sleep(3)
        except anthropic.RateLimitError:
            wait = min(30 * (2 ** attempt), 300)
            print(f"  [RATE LIMIT] 等待 {wait}s ({attempt+1}/6)")
            time.sleep(wait)
        except Exception as e:
            print(f"  [ERROR] API 调用失败 ({attempt+1}): {e}")
            time.sleep(5)
    return None


# ─── 数据库 ──────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            video_name                TEXT NOT NULL,
            folder_path               TEXT NOT NULL,
            location_name             TEXT,
            label_time                TIMESTAMP,
            collection_time           TEXT,
            top_road_category         TEXT,
            top_road_subcategory      TEXT,
            secondary_tags_json       TEXT,
            has_dynamic_override      INTEGER DEFAULT 0,
            has_atmosphere_override   INTEGER DEFAULT 0,
            has_road_surface_override INTEGER DEFAULT 0,
            duration                  REAL DEFAULT 0,
            quality_tags              TEXT DEFAULT '',
            comments                  TEXT DEFAULT '',
            image_path                TEXT DEFAULT ''
        )
    """)
    # 兼容旧版 auto_labeled.db（可能缺少 image_path 列）
    try:
        conn.execute("ALTER TABLE dataset ADD COLUMN image_path TEXT DEFAULT ''")
    except Exception:
        pass
    conn.commit()


def existing_paths(conn: sqlite3.Connection) -> set:
    return {r[0] for r in conn.execute("SELECT folder_path FROM dataset").fetchall()}


def write_location(conn: sqlite3.Connection, loc: dict, vlm_result: dict):
    folder_path = f"PHOTO/{loc['city']}/{loc['location_name']}"
    tags = vlm_result.get("tags", {})
    # 把 scene_type 和 flight_height 放进 comments 方便展示
    comments = f"场景:{loc['scene_type']} | 飞行高度:{loc['flight_height']}m | 数据量:{loc['data_size_gb']}GB"

    conn.execute("""
        INSERT OR REPLACE INTO dataset
        (video_name, folder_path, location_name, label_time, collection_time,
         top_road_category, top_road_subcategory, secondary_tags_json,
         has_dynamic_override, has_atmosphere_override, has_road_surface_override,
         duration, quality_tags, comments, image_path)
        VALUES (?,?,?,?,?,?,?,?,0,0,0,?,'',?,?)
    """, (
        loc["photo"],
        folder_path,
        loc["location_name"],
        datetime.now().isoformat(),
        "",                          # 图片地点无采集时间戳
        vlm_result.get("top_road_category", ""),
        vlm_result.get("top_road_subcategory", ""),
        json.dumps(tags, ensure_ascii=False),
        0.0,                         # 无视频时长数据
        comments,
        str(PHOTO_DIR / loc["photo"]),
    ))
    conn.commit()


# ─── 主流程 ──────────────────────────────────────────────────────────────

def main():
    api_key  = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN")
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.mczbc.cn")

    client = anthropic.Anthropic(api_key=api_key, base_url=base_url)
    print(f"[API] 端点: {base_url}")

    # 加载断点缓存
    cache: dict = {}
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"[缓存] 已有 {len(cache)} 条 VLM 结果")

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    done_paths = existing_paths(conn)

    total  = len(PHOTO_LOCATIONS)
    saved  = 0
    skipped = 0

    for i, loc in enumerate(PHOTO_LOCATIONS, 1):
        folder_path = f"PHOTO/{loc['city']}/{loc['location_name']}"
        print(f"\n[{i}/{total}] {loc['city']} · {loc['location_name']}  ({loc['photo']})")

        # 已在 DB 中则跳过（除非强制重跑）
        if folder_path in done_paths:
            print(f"  → 已存在于数据库，跳过（如需重新标注请删除该记录）")
            skipped += 1
            continue

        # 从缓存取或调用 VLM
        if folder_path in cache:
            print(f"  → 使用缓存结果")
            vlm_result = cache[folder_path]
        else:
            print(f"  → 调用 VLM 标注...")
            vlm_result = call_vlm(client, loc)
            if vlm_result is None:
                print(f"  [FAIL] 标注失败，跳过")
                continue
            cache[folder_path] = vlm_result
            # 保存缓存
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)

        write_location(conn, loc, vlm_result)
        saved += 1
        print(f"  ✓ 已写入 DB | {vlm_result.get('top_road_category')}/{vlm_result.get('top_road_subcategory')}")

    conn.close()
    print(f"\n完成！新写入 {saved} 条，跳过 {skipped} 条（已存在）。")
    print(f"数据库：{DB_FILE}")


if __name__ == "__main__":
    main()
