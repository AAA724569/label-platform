"""
修复 PHOTO 地点数据库：
1. 更新 6 条已有记录的 image_path（旧文件名 → 新文件名）
2. 插入 5 条缺失地点（先尝试 VLM API，失败则使用基于视觉分析的预设标签）
"""

import base64, io, json, os, re, sqlite3, time
from datetime import datetime
from pathlib import Path

os.environ.setdefault("ANTHROPIC_API_KEY", "sk-3iebUvSCeu1TO0F5JYJl23BE0U0VKBnLOjShTEfI8ZM34DMb")

import anthropic
from PIL import Image

PHOTO_DIR = Path("/home/stu6/projects/LabelWork1/photo")
DB_FILE   = "/home/stu6/projects/LabelWork1/label_platform.db"
MODEL     = "claude-haiku-4-5-20251001"
MAX_SIDE  = 1024

# ─── 文件名更正映射（旧 → 新）────────────────────────────────────────────
PATH_FIXES = {
    "PHOTO/十堰/园林路":       "shiyan2.png",
    "PHOTO/抚州/龙山大道":     "fuzhou2.PNG",   # fuzhou2 = 抚州第2行
    "PHOTO/抚州/街心花园":     "fuzhou3.PNG",   # fuzhou3 = 抚州第3行
    "PHOTO/贵港/北环金港立交": "guigang2.PNG",
    "PHOTO/贵港/工园支路":     "guigang3.jpg",
    "PHOTO/安阳/集市路":       "anyang1.png",
}

# ─── 缺失的 5 个地点 ─────────────────────────────────────────────────────
MISSING = [
    {
        "photo": "shiyan1.PNG",
        "city": "十堰", "scene_type": "环岛+高速路出入口",
        "location_name": "麻安高速", "flight_height": 200, "data_size_gb": 20.42,
    },
    {
        "photo": "fuzhou1.PNG",
        "city": "抚州", "scene_type": "T型路口",
        "location_name": "旧小区", "flight_height": 100, "data_size_gb": 30.0,
    },
    {
        "photo": "guigangjingangdadao20.PNG",
        "city": "贵港", "scene_type": "拥挤路段",
        "location_name": "金港大道", "flight_height": 120, "data_size_gb": 10.0,
    },
    {
        "photo": "69026c6b610ce86f3efcb66b44ac6486.jpg",
        "city": "贵港", "scene_type": "合流区",
        "location_name": "北环金港匝道", "flight_height": 120, "data_size_gb": 30.0,
    },
    {
        "photo": "anyang2.jpg",
        "city": "安阳", "scene_type": "高速路",
        "location_name": "台辉高速", "flight_height": 120, "data_size_gb": 0.45,
    },
]

# ─── 预设 fallback 标签（基于视觉分析，供 API 失败时使用）─────────────
FALLBACK_TAGS = {
    "PHOTO/十堰/麻安高速": {
        "top_road_category": "公路", "top_road_subcategory": "一级公路",
        "tags": {
            "一、道路静态环境": {
                "1.2 道路表面": {"表面类型": "沥青", "表面状态": "干燥"},
                "1.3 道路几何": {"坡度": "平路", "曲率": ["弯道 (曲率<0.01)"], "横坡": "正常排水坡度"},
                "1.4 包含车道特征": {"最宽车道数量": "四车道及以上",
                                    "车道类型": ["普通车道", "汇入匝道", "汇出匝道"], "车道宽度": ["标准"]},
                "1.5 道路边缘": {"边缘类型": "护栏 (混凝土)"},
                "1.6 道路交叉": {"交叉类型": ["小型环岛"]},
            },
            "二、交通设施": {
                "2.1 交通控制": {"信号灯": "无", "标志牌": "指示", "地面标签": ["实线", "导流线"]},
                "2.2 路侧与周边环境": {"设施": ["路边树木", "路灯"]},
                "2.3 特殊设施": {"类型": "无"},
            },
            "三、动态目标 (路面状况)": {
                "3.1 机动车": {"类型": ["轿车", "客车/巴士"]},
                "3.2 VRU": {"类型": ["无"]},
                "3.3 动物": {"类型": "无"},
                "3.4 障碍物": {"类型": ["无"]},
                "3.5 事故车辆": {"类型": "无"},
            },
            "四、大气环境": {
                "4.1 天气": {"类型": "晴"},
                "4.2 颗粒物": {"类型": "无"},
                "4.3 光照": {"来源": "自然光", "强度": "正常"},
                "4.4 气温": {"估算": "舒适 (-10℃ ~ 10℃)"},
            },
        },
    },
    "PHOTO/抚州/旧小区": {
        "top_road_category": "城市道路", "top_road_subcategory": "次干路",
        "tags": {
            "一、道路静态环境": {
                "1.2 道路表面": {"表面类型": "沥青", "表面状态": "干燥"},
                "1.3 道路几何": {"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"},
                "1.4 包含车道特征": {"最宽车道数量": "四车道及以上",
                                    "车道类型": ["普通车道", "非机动车道"], "车道宽度": ["标准"]},
                "1.5 道路边缘": {"边缘类型": "路缘石"},
                "1.6 道路交叉": {"交叉类型": ["平面交叉 (十字)"]},
            },
            "二、交通设施": {
                "2.1 交通控制": {"信号灯": "有", "标志牌": "限速",
                                "地面标签": ["实线", "虚线", "斑马线", "导流线"]},
                "2.2 路侧与周边环境": {"设施": ["路灯", "居民楼", "路边停车位"]},
                "2.3 特殊设施": {"类型": "无"},
            },
            "三、动态目标 (路面状况)": {
                "3.1 机动车": {"类型": ["轿车", "客车/巴士"]},
                "3.2 VRU": {"类型": ["行人", "电动车"]},
                "3.3 动物": {"类型": "无"},
                "3.4 障碍物": {"类型": ["无"]},
                "3.5 事故车辆": {"类型": "无"},
            },
            "四、大气环境": {
                "4.1 天气": {"类型": "多云"},
                "4.2 颗粒物": {"类型": "无"},
                "4.3 光照": {"来源": "自然光", "强度": "正常"},
                "4.4 气温": {"估算": "舒适 (-10℃ ~ 10℃)"},
            },
        },
    },
    "PHOTO/贵港/金港大道": {
        "top_road_category": "城市道路", "top_road_subcategory": "主干路",
        "tags": {
            "一、道路静态环境": {
                "1.2 道路表面": {"表面类型": "沥青", "表面状态": "干燥"},
                "1.3 道路几何": {"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"},
                "1.4 包含车道特征": {"最宽车道数量": "四车道及以上",
                                    "车道类型": ["普通车道"], "车道宽度": ["标准"]},
                "1.5 道路边缘": {"边缘类型": "护栏 (混凝土)"},
                "1.6 道路交叉": {"交叉类型": ["路段 (无交叉)"]},
            },
            "二、交通设施": {
                "2.1 交通控制": {"信号灯": "无", "标志牌": "无",
                                "地面标签": ["实线", "双黄线"]},
                "2.2 路侧与周边环境": {"设施": ["路灯", "路边树木", "绿化带"]},
                "2.3 特殊设施": {"类型": "无"},
            },
            "三、动态目标 (路面状况)": {
                "3.1 机动车": {"类型": ["轿车", "客车/巴士", "卡车/货车"]},
                "3.2 VRU": {"类型": ["无"]},
                "3.3 动物": {"类型": "无"},
                "3.4 障碍物": {"类型": ["无"]},
                "3.5 事故车辆": {"类型": "无"},
            },
            "四、大气环境": {
                "4.1 天气": {"类型": "多云"},
                "4.2 颗粒物": {"类型": "无"},
                "4.3 光照": {"来源": "自然光", "强度": "正常"},
                "4.4 气温": {"估算": "炎热 (10℃ ~ 20℃)"},
            },
        },
    },
    "PHOTO/贵港/北环金港匝道": {
        "top_road_category": "城市道路", "top_road_subcategory": "主干路",
        "tags": {
            "一、道路静态环境": {
                "1.2 道路表面": {"表面类型": "沥青", "表面状态": "干燥"},
                "1.3 道路几何": {"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"},
                "1.4 包含车道特征": {"最宽车道数量": "四车道及以上",
                                    "车道类型": ["普通车道", "汇入匝道"], "车道宽度": ["标准"]},
                "1.5 道路边缘": {"边缘类型": "护栏 (混凝土)"},
                "1.6 道路交叉": {"交叉类型": ["路段 (无交叉)"]},
            },
            "二、交通设施": {
                "2.1 交通控制": {"信号灯": "无", "标志牌": "指示",
                                "地面标签": ["实线", "导流线", "虚线"]},
                "2.2 路侧与周边环境": {"设施": ["路灯", "绿化带"]},
                "2.3 特殊设施": {"类型": "无"},
            },
            "三、动态目标 (路面状况)": {
                "3.1 机动车": {"类型": ["轿车", "客车/巴士", "卡车/货车"]},
                "3.2 VRU": {"类型": ["行人"]},
                "3.3 动物": {"类型": "无"},
                "3.4 障碍物": {"类型": ["无"]},
                "3.5 事故车辆": {"类型": "无"},
            },
            "四、大气环境": {
                "4.1 天气": {"类型": "晴"},
                "4.2 颗粒物": {"类型": "无"},
                "4.3 光照": {"来源": "自然光", "强度": "正常"},
                "4.4 气温": {"估算": "炎热 (10℃ ~ 20℃)"},
            },
        },
    },
    "PHOTO/安阳/台辉高速": {
        "top_road_category": "公路", "top_road_subcategory": "高速公路",
        "tags": {
            "一、道路静态环境": {
                "1.2 道路表面": {"表面类型": "沥青", "表面状态": "干燥"},
                "1.3 道路几何": {"坡度": "平路", "曲率": ["直线"], "横坡": "正常排水坡度"},
                "1.4 包含车道特征": {"最宽车道数量": "四车道及以上",
                                    "车道类型": ["普通车道", "应急车道", "汇入匝道"], "车道宽度": ["标准"]},
                "1.5 道路边缘": {"边缘类型": "护栏 (金属)"},
                "1.6 道路交叉": {"交叉类型": ["立体交叉"]},
            },
            "二、交通设施": {
                "2.1 交通控制": {"信号灯": "无", "标志牌": "限速",
                                "地面标签": ["实线", "虚线", "导流线"]},
                "2.2 路侧与周边环境": {"设施": ["路灯", "路边树木"]},
                "2.3 特殊设施": {"类型": "无"},
            },
            "三、动态目标 (路面状况)": {
                "3.1 机动车": {"类型": ["轿车", "客车/巴士"]},
                "3.2 VRU": {"类型": ["无"]},
                "3.3 动物": {"类型": "无"},
                "3.4 障碍物": {"类型": ["无"]},
                "3.5 事故车辆": {"类型": "无"},
            },
            "四、大气环境": {
                "4.1 天气": {"类型": "晴"},
                "4.2 颗粒物": {"类型": "无"},
                "4.3 光照": {"来源": "自然光", "强度": "正常"},
                "4.4 气温": {"估算": "寒冷 (-20℃ ~ -10℃)"},
            },
        },
    },
}

# ─── VLM 调用 ────────────────────────────────────────────────────────────
LABEL_SCHEMA = json.loads(open("/dev/stdin").read()) if False else None  # 不再重复定义

TOP_LEVEL_CONFIG = {
    "区域": ["封闭园区", "交通管制区域", "开放道路"],
    "城市道路": ["快速路", "主干路", "次干路", "支路", "街巷"],
    "公路": ["高速公路", "一级公路", "二级公路", "三级公路", "四级公路"],
    "乡村道路": ["村道", "其他乡村内部道路"],
    "其他道路": ["厂矿", "林区", "港口", "专用道路"],
    "停车区域": ["室内停车场", "室外停车场", "路侧停车位"],
    "自动驾驶场景": ["封闭场景", "半封闭场景", "开放场景"],
}

_FULL_SCHEMA = {
    "一、道路静态环境": {
        "1.2 道路表面": {"表面类型": ["沥青","混凝土","土路","碎石","冰雪路面","金属板"],
                        "表面状态": ["干燥","潮湿","积水","积雪","结冰","泥泞"]},
        "1.3 道路几何": {"坡度": ["平路","上坡","下坡","起伏路"],
                        "曲率": ["直线","弯道 (曲率<0.01)","弯道 (0.01<曲率<0.05)","弯道 (曲率>0.05)"],
                        "横坡": ["正常排水坡度","反超高","无横坡"]},
        "1.4 包含车道特征": {"最宽车道数量": ["单车道","双车道","三车道","四车道及以上"],
                            "车道类型": ["普通车道","公交专用道","HOV车道","潮汐车道","应急车道",
                                        "非机动车道","人行道","汇入匝道","汇出匝道"],
                            "车道宽度": ["标准","狭窄","超宽"]},
        "1.5 道路边缘": {"边缘类型": ["路缘石","护栏 (金属)","护栏 (混凝土)","草地/泥土","无物理隔离"]},
        "1.6 道路交叉": {"交叉类型": ["路段 (无交叉)","平面交叉 (十字)","平面交叉 (丁字)",
                                    "平面交叉 (畸形)","大型环岛 (出入口数 > 4)","小型环岛","立体交叉"]},
    },
    "二、交通设施": {
        "2.1 交通控制": {"信号灯": ["有","无"], "标志牌": ["限速","禁止","指示","警告","施工","无"],
                        "地面标签": ["实线","虚线","双黄线","导流线","斑马线","标线磨损"]},
        "2.2 路侧与周边环境": {"设施": ["无","路灯","电线杆","隔音墙","路边树木","路边停车位",
                                        "地面停车场出入口","隧道出入口","居民楼","商场","学校",
                                        "医院","公园","绿化带"]},
        "2.3 特殊设施": {"类型": ["收费站","检查站","施工区域围挡","减速带","无"]},
    },
    "三、动态目标 (路面状况)": {
        "3.1 机动车": {"类型": ["轿车","客车/巴士","卡车/货车","特种车辆 (警)","特种车辆(消)","特种车辆(救)","工程车辆"]},
        "3.2 VRU": {"类型": ["自行车","电动车","三轮车","行人","无"]},
        "3.3 动物": {"类型": ["有","无"]},
        "3.4 障碍物": {"类型": ["落石","遗洒物","倒伏树木","锥桶","无"]},
        "3.5 事故车辆": {"类型": ["有","无"]},
    },
    "四、大气环境": {
        "4.1 天气": {"类型": ["晴","多云","阴","雨 (小/中/大)","雪","雾","冰雹"]},
        "4.2 颗粒物": {"类型": ["无","雾霾","沙尘","烟尘"]},
        "4.3 光照": {"来源": ["自然光","人工照明","混合光"], "强度": ["正常","强光/逆光","弱光/昏暗","黑暗"]},
        "4.4 气温": {"估算": ["极寒 (< -20℃)","寒冷 (-20℃ ~ -10℃)","舒适 (-10℃ ~ 10℃)",
                            "炎热 (10℃ ~ 20℃)","极热 (> 20℃)"]},
    },
}


def resize_image(path: str) -> tuple[bytes, str]:
    with Image.open(path) as img:
        w, h = img.size
        if max(w, h) > MAX_SIDE:
            scale = MAX_SIDE / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=88)
        return buf.getvalue(), "image/jpeg"


def try_vlm(client, loc: dict) -> dict | None:
    img_path = PHOTO_DIR / loc["photo"]
    if not img_path.exists():
        return None
    schema_str = json.dumps(_FULL_SCHEMA, ensure_ascii=False, indent=2)
    top_str    = json.dumps(TOP_LEVEL_CONFIG, ensure_ascii=False, indent=2)
    prompt = f"""你是一名自动驾驶数据标注专家。这是无人机航拍道路图像。
地点：{loc['city']} · {loc['location_name']}（场景:{loc['scene_type']}，高度:{loc['flight_height']}m）

按标签体系输出 JSON，不要多余文字：
顶层：{top_str}
次级：{schema_str}
多选字段返回列表：曲率、车道类型、车道宽度、地面标签、设施、3.1机动车类型、3.2VRU类型、3.4障碍物类型、交叉类型

输出格式：
{{"top_road_category":"城市道路","top_road_subcategory":"主干路","tags":{{...}}}}"""

    raw, mime = resize_image(str(img_path))
    content = [
        {"type": "image", "source": {"type": "base64", "media_type": mime,
                                     "data": base64.standard_b64encode(raw).decode()}},
        {"type": "text", "text": prompt},
    ]
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=MODEL, max_tokens=2500,
                messages=[{"role": "user", "content": content}]
            )
            text = next((b.text.strip() for b in resp.content if b.type == "text"), "")
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                r = json.loads(m.group())
                if "top_road_category" in r and "tags" in r:
                    return r
            print(f"  [WARN] 非预期响应({attempt+1}): {text[:120]!r}")
            time.sleep(2)
        except Exception as e:
            print(f"  [ERROR] ({attempt+1}): {e}")
            time.sleep(3)
    return None


def write_row(conn, loc: dict, result: dict, source: str):
    fp = f"PHOTO/{loc['city']}/{loc['location_name']}"
    comments = (f"场景:{loc['scene_type']} | 高度:{loc['flight_height']}m "
                f"| 数据:{loc['data_size_gb']}GB | 标注:{source}")
    conn.execute("""
        INSERT OR REPLACE INTO dataset
        (video_name, folder_path, location_name, label_time, collection_time,
         top_road_category, top_road_subcategory, secondary_tags_json,
         has_dynamic_override, has_atmosphere_override, has_road_surface_override,
         duration, quality_tags, comments, image_path)
        VALUES (?,?,?,?,?,?,?,?,0,0,0,0,'',?,?)
    """, (
        loc["photo"], fp, loc["location_name"],
        datetime.now().isoformat(), "",
        result["top_road_category"], result["top_road_subcategory"],
        json.dumps(result["tags"], ensure_ascii=False),
        comments, str(PHOTO_DIR / loc["photo"]),
    ))
    conn.commit()


def main():
    conn = sqlite3.connect(DB_FILE)

    # ── 步骤 1：修复已有 6 条记录的 image_path ──
    print("=== 步骤1：修复 image_path ===")
    for fp, new_photo in PATH_FIXES.items():
        new_path = str(PHOTO_DIR / new_photo)
        conn.execute("UPDATE dataset SET image_path=?, video_name=? WHERE folder_path=?",
                     (new_path, new_photo, fp))
    conn.commit()
    print(f"  已更新 {len(PATH_FIXES)} 条 image_path")

    # ── 步骤 2：插入 5 条缺失记录 ──
    print("\n=== 步骤2：插入缺失地点 ===")
    existing = {r[0] for r in conn.execute("SELECT folder_path FROM dataset").fetchall()}

    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.mczbc.cn")
    client = anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        base_url=base_url,
    )

    for loc in MISSING:
        fp = f"PHOTO/{loc['city']}/{loc['location_name']}"
        if fp in existing:
            print(f"  已存在: {fp}，跳过")
            continue

        print(f"  处理: {loc['city']} · {loc['location_name']}  [{loc['photo']}]")
        print(f"    → 尝试 VLM...")
        result = try_vlm(client, loc)

        if result:
            source = "VLM自动"
            print(f"    ✓ VLM成功: {result['top_road_category']}/{result['top_road_subcategory']}")
        else:
            fallback = FALLBACK_TAGS.get(fp)
            if fallback:
                result = fallback
                source = "预设标签(需复核)"
                print(f"    → 使用预设标签: {result['top_road_category']}/{result['top_road_subcategory']}")
            else:
                print(f"    [SKIP] 无预设标签，跳过")
                continue

        write_row(conn, loc, result, source)
        print(f"    ✓ 已写入 DB")

    conn.close()

    # ── 验证 ──
    print("\n=== 验证结果 ===")
    conn2 = sqlite3.connect(DB_FILE)
    rows = conn2.execute(
        "SELECT folder_path, location_name, top_road_category, image_path FROM dataset WHERE folder_path LIKE 'PHOTO/%' ORDER BY folder_path"
    ).fetchall()
    for r in rows:
        img_ok = "✓" if Path(r[3] or "").exists() else "✗(图片不存在)"
        print(f"  {r[0]}  {r[2]}  {img_ok}")
    print(f"\n共 {len(rows)} 条 PHOTO 记录")
    conn2.close()


if __name__ == "__main__":
    main()
