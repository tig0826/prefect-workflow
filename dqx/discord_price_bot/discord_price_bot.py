import discord
import re
import requests
import os
from discord.ext import commands
from itertools import product
import unicodedata
import math
import logging


# Prefect API のエンドポイント
PREFECT_API_URL = "http://prefect.mynet/api"
PREFECT_DEPLOYMENT_ID = "fa138ec0-52d9-4f52-bb33-7c7ccebbfcbb"
PREFECT_DEPLOYMENT_SEND_PRICE_IMAGE_ID = "29d78ce3-54ed-49e8-9850-3aadb3472a0c"
PREFECT_DEPLOYMENT_SEND_CHEAP_SAIBOU_PRICE_ID = "b7b275d4-1b75-4c27-8bcc-501b5c876377"

# 環境変数から Discord Bot Token を取得
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# Intents 設定
intents = discord.Intents.default()
intents.typing = False
intents.presences = False
intents.members = False
intents.message_content = True  # メッセージ内容を取得できるようにする (重要)
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f'{bot.user} としてログインしました！')


def normalize_message(text):
    # 全角スペース、カンマなどを半角に正規化
    text = unicodedata.normalize('NFKC', text)
    text = text.replace("、", ",")
    text = text.replace("レボ,", "レボル,")
    text = text.replace("レボ)", "レボル)")
    text = text.replace("fb", "FB")
    text = text.replace("フォースブレイク", "FB")
    text = text.replace("ds", "DS")
    text = text.replace("ダークネスショット", "DS")
    text = text.replace("ダクショ", "DS")
    text = text.replace("ダークネス", "DS")
    text = text.replace("アヌ,", "アヌビス")
    text = text.replace("アヌ)", "アヌビス)")
    text = text.replace("さいか", "災禍")
    text = text.replace("じゅもん", "呪")
    text = text.replace("呪文", "呪")
    text = text.replace("呪文耐性", "呪")
    text = text.replace("炎耐性", "炎")
    text = text.replace("風耐性", "風")
    text = text.replace("闇耐性", "闇")
    text = text.replace("ブレス耐性", "ブレス")
    text = text.replace("ぶれす", "ブレス")
    return text


def parse_resistance_dogu(msg):
    msg = normalize_message(msg)
    pattern = r'(防具|料理|軽減)\(([^\)]+)\)'
    matches = re.findall(pattern, msg)
    result = {
        '防具': {'呪': 0, '炎': 0, '風': 0},
        '料理': {'呪': 0, '炎': 0, '風': 0},
        '軽減': 22
    }
    for category, values in matches:
        items = values.split(',')
        for item in items:
            item = item.strip()
            if category == '軽減':
                # 式をそのまま評価できるように修正
                try:
                    result['軽減'] = int(eval(item))
                except Exception as e:
                    print(f"軽減の式でエラー: {item} → {e}")
            else:
                res_type_match = re.match(r'[^\d\+\*\/\-\s]+', item)
                value_expr_match = re.search(r'[\d\+\-\*/\.]+', item)
                if res_type_match and value_expr_match:
                    res_type = res_type_match.group()
                    value_expr = value_expr_match.group()
                    try:
                        value = int(eval(value_expr))
                        result[category][res_type] = value
                    except Exception as e:
                        print(f"{category} の {item} 式でエラー: {e}")
    return result


def calculate_spell_damage_dogu(reduction, reduc_spell=0, reduc_fire=0, reduc_wind=0, makekkai_num=0, yaiba=False, is_max=False):
    # メラガイアーのダメージ計算
    base_damage = 1750 if not is_max else math.ceil(1750*1.0625)
    damage_mera = (base_damage*(1-(1-(1-0.25*yaiba)*(1-0.2*makekkai_num) +
                   reduc_spell+0.06*yaiba))*(1-reduc_fire) - reduction)
    # damageを正の整数にする
    damage_mera = max(0, int(damage_mera))
    # 太古に封じられしバギのダメージ計算
    base_damage = 2000 if not is_max else math.ceil(2000*1.0625)
    damage_bagi = (base_damage*(1-(1-(1-0.25*yaiba)*(1-0.2*makekkai_num) +
                   reduc_spell+0.06*yaiba))*(1-reduc_wind) - reduction)
    damage_bagi = max(0, int(damage_bagi))
    damage_data = {
        'メラガイアー': damage_mera,
        'バギ': damage_bagi
    }
    return damage_data


def create_spell_damage_message_dogu(message):
    u"""
    discordから受け取った耐性情報を解析してダメージ計算を行った結果を返す関数
    """
    resistance_data = parse_resistance_dogu(message)
    armor_data = resistance_data['防具']  # 防具の耐性
    meal_data = resistance_data['料理']  # 料理の耐性
    reduction = resistance_data['軽減']  # 軽減値
    # ダメージ計算
    reduc_spell = (armor_data['呪'] + 6)/100
    reduc_fire = (armor_data['炎'] + meal_data['炎'] + 29 + 6)/100
    reduc_wind = (armor_data['風'] + meal_data['風'] + 6)/100
    # 魔結界、やいばの各パターンの被ダメージ計算
    patterns = [(makekkai_num, yaiba) for makekkai_num in range(3)
                for yaiba in (False, True)]
    damage_results = {
        (makekkai_num, yaiba): calculate_spell_damage_dogu(reduction, reduc_spell, reduc_fire, reduc_wind, makekkai_num, yaiba)
        for makekkai_num, yaiba in patterns
    }
    damage_results_max = {
        (makekkai_num, yaiba): calculate_spell_damage_dogu(reduction, reduc_spell, reduc_fire, reduc_wind, makekkai_num, yaiba, is_max=True)
        for makekkai_num, yaiba in patterns
    }
    lines = [
        "🔹 現在の耐性情報",
        f"呪文耐性: {reduc_spell*100:.1f}%",
        f"炎耐性: {reduc_fire*100:.1f}%",
        f"風耐性: {reduc_wind*100:.1f}%",
        f"ダメージ軽減: {reduction}",
        "----------------------",
        "🔹 ダメージ計算結果",
        "🔥 神速メラガイアー"
    ]
    for m in range(3):
        for y in (False, True):
            label = f"魔結界{m} やいば{'◯' if y else '✕'}"
            val = damage_results[(m, y)]['メラガイアー']
            val_max = damage_results_max[(m, y)]['メラガイアー']
            lines.append(
                f"{label}: 平均: {val}✕3 計: {val * 3},   最大: {val_max}✕3 計: {val_max * 3}")
    lines += ["----------------------", "🌪️ 太古に封じられしバギ"]
    for m in range(3):
        for y in (False, True):
            label = f"魔結界{m} やいば{'◯' if y else '✕'}"
            val = damage_results[(m, y)]['バギ']
            val_max = damage_results_max[(m, y)]['バギ']
            lines.append(
                f"{label}: 平均: {val}✕2 計: {val * 2},   最大: {val_max}✕2 計: {val_max * 2}")
    lines.append("----------------------")
    message_text = "\n".join(lines)
    return message_text[:2000]  # Discord のメッセージは2000文字まで


def parse_attack_message_dogu(msg):
    msg = normalize_message(msg)
    # 初期状態（光、竜、補正系は0）
    result = {
        'job': None,
        'attack': None,
        '光': 0,
        '竜': 0,
        '補正': {
            'FB': [1, 2],
            'ルカニ': [0, 1, 2],
            'DS': [False, True],
            'レボル': [False, True],
            '災禍': [False],
            'アヌビス': [False],
            '牙神': [True]
        }
    }
    # 職・攻撃・光・竜 を抽出
    basic_pattern = r'(職|攻撃|光|竜)\((\w+)\)'
    for key, val in re.findall(basic_pattern, msg):
        if key == '職':
            result['job'] = val
        elif key == '攻撃':
            result['attack'] = int(val)
        else:
            result[key] = int(val)
    # 有効補正（FB, ルカニは2、それ以外は1）
    effective_match = re.search(r'有効\((.*?)\)', msg)
    if effective_match:
        for item in effective_match.group(1).split(','):
            item = item.strip()
            if item in result['補正']:
                result['補正'][item] = [2] if item in ['FB', 'ルカニ'] else [1]
    # 無効補正（0にする）
    ineffective_match = re.search(r'無効\((.*?)\)', msg)
    if ineffective_match:
        for item in ineffective_match.group(1).split(','):
            item = item.strip()
            if item in result['補正']:
                result['補正'][item] = [0]
    ineffective_match = re.search(r'失敗\((.*?)\)', msg)
    if ineffective_match:
        for item in ineffective_match.group(1).split(','):
            item = item.strip()
            if item in result['補正']:
                if item in ['FB', 'ルカニ']:
                    result['補正'][item] = [1]
    if result['job'] == "レン":
        result['job'] = "レンジャー"
    elif result['job'] == "盗":
        result['job'] = "盗賊"
    elif result['job'] == "魔戦" or result['job'] == "マセン":
        result['job'] = "魔法戦士"
    return result


def calculate_attack_damage(atk, defense, skill_mult, elem_mult, race_mult, damage_up_mult, enemy_damage_taken_mult, enemy_element_down_mult, damage_add, is_god_smash=False):
    # 基礎ダメージ
    if is_god_smash:
        # ゴッドスマッシュは守備力に関わらず以下の計算に従う
        if atk < 300:
            base_damage = 800
        elif atk < 1000:
            base_damage = (atk-300)*2+800
        else:
            base_damage = 2200
    else:
        if atk > defense*(4/7):
            base_damage = atk/2 - defense/4
        elif atk > defense*(1/2):
            base_damage = max(0, atk/16)
        else:
            base_damage = 0.5
    damage = [min(9999, s_mult*base_damage*elem_mult*race_mult*damage_up_mult *
                  enemy_damage_taken_mult*enemy_element_down_mult+damage_add) for s_mult in skill_mult]
    # return f"{base_damage}*{elem_mult}*{race_mult}*{damage_up_mult}*{enemy_damage_taken_mult}*{enemy_element_down_mult}+{damage_add}"
    return [max(int(d), 0) for d in damage]


def calculate_baikilt(atk, power, weapon_atk):
    # バイキルトがかかった際の攻撃力を計算する関数
    # バイキルトはちからと武器の錬金・輝晶効果を除いた基礎攻撃力を1.4倍する
    atk_baiki = atk + (power+weapon_atk) * 0.4
    return atk_baiki


def calculate_defense(defense, rukani, anubis):
    # 敵の守備力を補正
    if anubis:
        defense_rukani = 0
    elif rukani == 0:
        defense_rukani = defense
    elif rukani == 1:
        defense_rukani = defense * 0.7
    elif rukani == 2:
        defense_rukani = defense * 0.6
    return defense_rukani


def create_attack_damage_message_dogu(message):
    attack_data = parse_attack_message_dogu(message)
    job = attack_data['job']
    atk = attack_data['attack']
    light_up = attack_data['光']
    dragon_up = attack_data['竜']
    # Discord返答メッセージを作成する関数
    u"""武器攻撃力には錬金、輝結効果は含めない
    職業スキルでライガークラッシュとサンライトアローのダメージアップは最大で計算
    各種特技の宝珠効果は最大で計算
    """
    send_message = "🔹 入力した情報\n"
    send_message += f"職業: {job}\n"
    send_message += f"攻撃力: {atk}\n"
    send_message += f"光特攻: {light_up}%\n"
    send_message += f"ドラゴン特攻: {dragon_up}%\n"
    send_message_list = [send_message]
    job_data = {}
    job_data['盗賊'] = {'力': 450, '武器攻撃力': 168, 'ダメージアップ': 10+50}
    job_data['レンジャー'] = {'力': 441, '武器攻撃力': 168, 'ダメージアップ': 10+0}
    job_data['魔法戦士'] = {'力': 448, '武器攻撃力': 198, 'ダメージアップ': 10+0}
    # 特技ごとのダメージ倍率
    skill_mult_data = {
        "ライガークラッシュ": [2.5, 2.5, 2.5, 2.5, 2.5],
        "シャイニングボウ": [0.75, 0.75, 0.75, 0.75, 0.75, 0.75],
        "サンライトアロー": [0.95, 0.95, 0.95, 0.95, 0.95],
        "ダークネスショット": [3.8],
        "サプライズラッシュ": [2.3],
        "ゴッドスマッシュ": [1.0],
        "サマーソルトクロー": [3.5],
    }
    # ドグドラのデータ
    defense = 867  # ドグドラの守備力
    light_def = 0.9  # ドグドラの光耐性
    # 属性ダメージ倍率
    elem_mult = 1+light_up*0.01
    # ドラゴン系に対するダメージ倍率
    # if job in ['盗賊', 'レンジャー']:
    #     race_mult = 1+dragon_up*0.01+0.15
    # else:
    #     race_mult = 1+dragon_up*0.01
    race_mult = 1+dragon_up*0.01
    # ダメージアップ倍率
    # 宝珠効果
    houju_mult = {
        "ライガークラッシュ": 0.18,
        "シャイニングボウ": 0.3,
        "サンライトアロー": 0.18,
        "ダークネスショット": 0.18,
        "サプライズラッシュ": 0.3,
        "ゴッドスマッシュ": 0.3,
        "サマーソルトクロー": 0.3,
    }
    # 職業スキル効果
    job_skill_mult = {
        "ライガークラッシュ": 0.05,
        "シャイニングボウ": 0.05,
        "サンライトアロー": 0.02,
        "ダークネスショット": 0,
        "サプライズラッシュ": 0,
        "ゴッドスマッシュ": 0,
        "サマーソルトクロー": 0,
    }
    damage_up_mult_data = {key: houju_mult.get(
        key, 0) + job_skill_mult.get(key, 0) for key in houju_mult.keys()}
    skill_list = {
        "盗賊": ["ライガークラッシュ", "サプライズラッシュ", "ゴッドスマッシュ"],
        "レンジャー": ["ライガークラッシュ", "サマーソルトクロー", "ゴッドスマッシュ"],
        "魔法戦士": ["シャイニングボウ", "サンライトアロー", "ダークネスショット"],
    }
    for skill in skill_list[job]:
        send_message = f"🔹 {skill}のダメージ\n"
        # FBとルカニの段階、災禍、レボル、牙神昇誕、アヌビス、ダークネスショットの有無のパターン毎に計算
        # FBの段階
        message_template = "FB{FB},ﾙｶﾆ{rukani},DS{DS},ﾚﾎﾞﾙ{revol},災禍{saika},ｱﾇﾋﾞｽ{anubis}: "
        # すべての組み合わせでメッセージを生成
        FB_values = attack_data['補正']['FB']
        rukani_values = attack_data['補正']['ルカニ']
        DS_values = attack_data['補正']['DS']
        revol_values = attack_data['補正']['レボル']
        saika = attack_data['補正']['災禍'][0]
        anubis = attack_data['補正']['アヌビス'][0]
        gashin = attack_data['補正']['牙神'][0]
        # 各変数の数値 → 記号への変換用辞書を定義
        convert_dict = {
            'FB': {0: '✕', 1: '△', 2: '◯'},
            'rukani': {0: '✕', 1: '△', 2: '◯'},
            'DS': {0: '✕', 1: '◯'},
            'revol': {0: '✕', 1: '◯'},
            'saika': {0: '✕', 1: '◯'},
            'anubis': {0: '✕', 1: '◯'},
        }
        if skill == "ゴッドスマッシュ":
            # ゴッドスマッシュはあいての守備力の影響を受けない
            rukani_values = [0]
            anubis = False
            message_template = "FB{FB},DS{DS},ﾚﾎﾞﾙ{revol},災禍{saika}: "
        if skill == "サマーソルトクロー":
            revol_values = [0]
        if skill == "ダークネスショット":
            DS_values = [0]
        for fb, rukani, ds, revol in product(FB_values, rukani_values, DS_values, revol_values):
            if skill == "ゴッドスマッシュ":
                msg_situation = message_template.format(
                    FB=convert_dict['FB'][fb],
                    DS=convert_dict['DS'][ds],
                    revol=convert_dict['revol'][revol],
                    saika=convert_dict['saika'][saika],
                )
            else:
                msg_situation = message_template.format(
                    FB=convert_dict['FB'][fb],
                    rukani=convert_dict['rukani'][rukani],
                    DS=convert_dict['DS'][ds],
                    revol=convert_dict['revol'][revol],
                    saika=convert_dict['saika'][saika],
                    anubis=convert_dict['anubis'][anubis]
                )
            # 攻撃力をバイキルトで補正
            power = job_data[job]['力']
            weapon_atk = job_data[job]['武器攻撃力']
            atk_baiki = calculate_baikilt(atk, power, weapon_atk)
            if job in ['盗賊', 'レンジャー']:
                atk_baiki += 60*gashin
            # 防御力を補正
            defense_rukani = calculate_defense(defense, rukani, anubis)
            skill_mult = skill_mult_data[skill]
            # ダメージアップ倍率
            if job == "盗賊":
                # 盗賊の場合、牙神昇誕にダメージ1.5倍の効果
                damage_up_mult = 1+damage_up_mult_data[skill] + 0.5*gashin
            else:
                damage_up_mult = 1+damage_up_mult_data[skill]
            # 属性ダメージアップはダークネスショットには乗らないので補正
            if skill == "ダークネスショット":
                elem_mult_mod = 1
            else:
                elem_mult_mod = elem_mult
            # 相手の被ダメージ上昇
            enemy_damage_taken_mult = 1+0.5*revol+0.5*saika
            # 相手の属性耐性ダウン
            enemy_element_down_mult = light_def+fb*0.5+ds*0.5
            damage_add = job_data[job]['ダメージアップ']
            is_god_smash = True if skill == "ゴッドスマッシュ" else False
            damage = calculate_attack_damage(atk_baiki, defense_rukani, skill_mult,
                                             elem_mult_mod, race_mult,
                                             damage_up_mult,
                                             enemy_damage_taken_mult,
                                             enemy_element_down_mult,
                                             damage_add, is_god_smash=is_god_smash)
            if len(damage) > 1:
                damage_message = f"平均:{int(sum(damage)/len(damage))}  計:{sum(damage)}"
            if len(damage) == 1:
                damage_message = str(damage[0])
            send_message += f"{msg_situation} " + damage_message + "\n"
        send_message_list.append(send_message)
        # # 各種データを表示する
        # print(f"職業: {job}")
        # print(f"攻撃力: {atk}")
        # print(f"光特攻: {light_up}%")
        # print(f"ドラゴン特攻: {dragon_up}%")
        # print(f"FB: {fb}, ルカニ: {rukani}, DS: {ds}, レボル: {revol}, 災禍: {saika}, アヌビス: {anubis}")
        # print(f"ダメージ: {damage}")
        # print(f"バイキルト後の攻撃力: {atk_baiki}")
        # print(f"敵の守備力: {defense_rukani}")
        # print(f"ダメージアップ倍率: {damage_up_mult}")
        # print(f"属性ダメージ倍率: {elem_mult_mod}")
        # print(f"敵の被ダメージ上昇倍率: {enemy_damage_taken_mult}")
        # print(f"敵の属性耐性ダウン倍率: {enemy_element_down_mult}")
        # print(f"ダメージ追加: {damage_add}")
        # if atk_baiki > defense_rukani*(4/7):
        #     base_damage = atk_baiki/2 - defense_rukani/4
        # elif atk_baiki > defense_rukani*(1/2):
        #     base_damage = max(0, atk_baiki/16)
        # else:
        #     base_damage = 0.5
        # print(f"基礎ダメージ: {base_damage}")
        # print("----------------------")
    return send_message_list


def parse_resistance_baja(msg):
    msg = normalize_message(msg)
    pattern = r'(防具|料理|軽減)\(([^\)]+)\)'
    matches = re.findall(pattern, msg)

    result = {
        '防具': {'呪': 0, 'ブレス': 0, '闇': 0},
        '料理': {'闇': 0},
        '軽減': 0
    }

    for category, values in matches:
        items = values.split(',')
        for item in items:
            item = item.strip()
            if category == '軽減':
                # 式をそのまま評価できるように修正
                try:
                    result['軽減'] = int(eval(item))
                except Exception as e:
                    print(f"軽減の式でエラー: {item} → {e}")
            else:
                res_type_match = re.match(r'[^\d\+\*\/\-\s]+', item)
                value_expr_match = re.search(r'[\d\+\-\*/\.]+', item)
                if res_type_match and value_expr_match:
                    res_type = res_type_match.group()
                    value_expr = value_expr_match.group()
                    try:
                        value = int(eval(value_expr))
                        result[category][res_type] = value
                    except Exception as e:
                        print(f"{category} の {item} 式でエラー: {e}")
    return result


def calculate_dark_damage_baja(reduction, reduc_spell=0, reduc_breath=0, reduc_dark=0, is_max=False):
    # 闇の流星のダメージ計算
    base_damage = 850 if not is_max else math.ceil(850*1.0625)
    damage_ryusei = base_damage*(1-reduc_dark) - reduction
    damage_ryusei = max(0, int(damage_ryusei))
    # ダークネスブレスのダメージ計算
    base_damage = 1000 if not is_max else math.ceil(1000*1.0625)
    damage_breath = base_damage*(1-reduc_dark)*(1-reduc_breath) - reduction
    damage_breath = max(0, int(damage_breath))
    # 連続ドルマドンのダメージ計算
    breath_doruma = 1100 if not is_max else math.ceil(1100*1.0625)
    damage_doruma = breath_doruma*(1-reduc_dark)*(1-reduc_spell) - reduction
    damage_doruma = max(0, int(damage_doruma))
    damage_data = {
        '闇の流星': damage_ryusei,
        'ダークネスブレス': damage_breath,
        '連続ドルマドン': damage_doruma
    }
    return damage_data


def create_dark_damage_message_baja(message):
    u"""
    discordから受け取った耐性情報を解析してダメージ計算を行った結果を返す関数
    """
    resistance_data = parse_resistance_baja(message)
    armor_data = resistance_data['防具']  # 防具の耐性
    meal_data = resistance_data['料理']  # 料理の耐性
    reduction = resistance_data['軽減']  # 軽減値
    # ダメージ計算
    reduc_spell = (armor_data['呪'] + 6)/100
    reduc_breath = (armor_data['ブレス'] + 6)/100
    reduc_dark = (armor_data['闇'] + meal_data['闇'] + 29 + 6)/100
    damage_results = calculate_dark_damage_baja(
        reduction, reduc_spell, reduc_breath, reduc_dark)
    damage_results_max = calculate_dark_damage_baja(
        reduction, reduc_spell, reduc_breath, reduc_dark, is_max=True)
    lines = [
        "🔹 現在の耐性情報",
        f"闇耐性: {reduc_dark*100:.1f}%",
        f"呪文耐性: {reduc_spell*100:.1f}%",
        f"ブレス耐性: {reduc_breath*100:.1f}%",
        f"ダメージ軽減: {reduction}",
        "----------------------",
        "🔹 ダメージ計算結果"]
    # 闇の流星のメッセージ
    dmg = damage_results['闇の流星']
    dmg_max = damage_results_max['闇の流星']
    msg = f"🌠 闇の流星: 平均: {dmg}, 最大: {dmg_max}"
    lines.append(msg)
    # ダークネスブレスのメッセージ
    dmg = damage_results['ダークネスブレス']
    dmg_max = damage_results_max['ダークネスブレス']
    msg = f"🌬️ ダークネスブレス: 平均: {dmg}, 最大: {dmg_max}"
    lines.append(msg)
    # 連続ドルマドンのメッセージ
    dmg = damage_results['連続ドルマドン']
    dmg_max = damage_results_max['連続ドルマドン']
    msg = f"🌑 連続ドルマドン: 平均: {dmg}×3 計:{dmg*3}, 最大: {dmg_max}×3 計:{dmg_max*3}"
    lines.append(msg)
    message_text = "\n".join(lines)
    logging.info(message_text)  # ログにメッセージを出力
    return message_text[:2000]  # Discord のメッセージは2000文字まで


@bot.event
async def on_message(message):
    # 自分自身のメッセージは無視
    if message.author == bot.user:
        return
    # ！で始まる場合は!になおす
    if message.content.startswith("！"):
        message.content = "!" + message.content[1:]
    # "!souba" メッセージを受信した場合
    if message.content.lower() == "!souba" or message.content.lower() == "!相場":
        print("価格情報のリクエストを受け取りました！")  # 追加：判定が通ってるか確認
        await message.channel.send("現在の価格を取得中です...")  # Discord に確認メッセージを送る
        # Prefect の Flow を実行 (skip_insert=True)
        response = requests.post(f"{PREFECT_API_URL}/deployments/{PREFECT_DEPLOYMENT_ID}/create_flow_run",
                                 json={"parameters": {"skip_insert": True}})
        # if response.status_code == 200:
        #     await message.channel.send("相場情報を取得しました！")
        # else:
        #     await message.channel.send("相場情報の取得に失敗しました...")
    elif message.content.lower().startswith("!グラフ") or message.content.lower().startswith("!graph"):
        if message.content.lower() == "!グラフ" or message.content.lower() == "!graph":
            send_message = "⚠️ このコマンドでグラフ情報を取得できます\n"
            send_message += "🎓️ 使い方: \n"
            send_message += "!グラフ <期間>もしくは !graph <期間>\n"
            send_message += "のようにコメントを書くとグラフ情報が取得されます\n"
            send_message += "例: !グラフ 1w\n"
            send_message += "この場合、1週間のグラフ情報を取得します\n"
            send_message += "※ 期間は◯d(◯日)、◯w(◯週間)、◯m(◯ヶ月)のいずれかを指定してください。\n"
            await message.channel.send(send_message)
        else:
            try:
                parts = message.content.split()
                if len(parts) != 2:
                    await message.channel.send("使い方: !グラフ <期間>もしくは !graph <期間>")
                    return
                period = parts[1]
                if period[-1] not in ['d', 'w', 'm']:
                    await message.channel.send("使い方: !グラフ <期間>もしくは !graph <期間>")
                    return
                # グラフ情報のリクエストを受け取った場合
                response = requests.post(f"{PREFECT_API_URL}/deployments/{PREFECT_DEPLOYMENT_SEND_PRICE_IMAGE_ID}/create_flow_run",
                                         json={"parameters": {"period": period}})
                if response.status_code != 201:
                    await message.channel.send(f"⚠️ Prefectの実行に失敗しました: {response.status_code}\n{response.text}")
                else:
                    await message.channel.send(f"✅ グラフ生成を開始しました（期間: {period}）")
            except Exception as e:
                await message.channel.send(f"⚠️ エラーが発生しました: `{e}`")
                return
    elif message.content.lower().startswith("!rieki") or message.content.lower().startswith("!利益"):
        try:
            parts = message.content.split()
            if len(parts) == 3:
                # 期待値を計算する場合
                price_saibou = float(parts[1])
                price_kaku = float(parts[2])
                profit = price_kaku*(1*0.1+(75/99)*0.5 +
                                     (45/99)*0.4)*0.95*4 - price_saibou*30
                profit = profit*10000
                profit = int(profit)
                profit = "{:,}".format(profit)
                await message.channel.send(f"一周持寄の利益期待値は{profit}G")
            elif len(parts) in (6, 7):
                # 共通の入力値
                price_saibou = float(parts[1])
                price_kaku = float(parts[2])
                num_45 = int(parts[3])
                num_75 = int(parts[4])
                num_kaku = int(parts[5])
                num_wipe = int(parts[6]) if len(parts) == 7 else 0
                num_all = num_45 + num_75 + num_kaku + num_wipe
                # 利益計算
                rate_45 = 45 / 99
                rate_75 = 75 / 99
                gain = price_kaku * (num_45 * rate_45 +
                                     num_75 * rate_75 + num_kaku)
                cost = price_saibou * 30 * num_all / 4
                profit = gain * 0.95 - cost
                profit = profit*10000
                profit = int(profit)
                profit = "{:,}".format(profit)
                await message.channel.send(f"{int(num_all/4)}周持寄で利益は{profit}G")
            else:
                # 使い方を送信
                send_message = "🎓️ 使い方: \n"
                send_message += "!rieki 細胞の値段(万G) 核の値段(万G)\n"
                send_message += "のようにコメントを書くと一周持ち寄りの場合の利益期待値が表示されます\n"
                send_message += "例: !rieki 7.2 102.3\n"
                send_message += "!rieki 細胞の値段(万G) 核の値段(万G) 45の数 75の数 核の数\n"
                send_message += "のようにコメントを書くと実際の利益が表示されます\n"
                send_message += "例: !rieki 8.05 119.3 3 9 4\n"
                send_message += "全滅した場合は!rieki 細胞の値段(万G) 核の値段(万G) 45の数 75の数 核の数 全滅数\n"
                await message.channel.send(send_message)
                return
        except ValueError:
            await message.channel.send("⚠️ 数値の入力に誤りがあります。正しい形式で入力してください。")
    elif message.content.lower() in ["!saibou", "!細胞"]:
        # やすい細胞の価格を取得
        print("価格情報のリクエストを受け取りました！")  # 追加：判定が通ってるか確認
        await message.channel.send("現在の価格を取得中です...")  # Discord に確認メッセージを送る
        try:
            response = requests.post(f"{PREFECT_API_URL}/deployments/{PREFECT_DEPLOYMENT_SEND_CHEAP_SAIBOU_PRICE_ID}/create_flow_run",
                                     json={})
            if response.status_code != 201:
                await message.channel.send(f"⚠️ Prefectの実行に失敗しました: {response.status_code}\n{response.text}")
                return
        except Exception as e:
            await message.channel.send(f"⚠️ エラーが発生しました: `{e}`")
            return
    elif message.content.lower().startswith("!ドグ呪文"):
        # helpの場合は使い方のメッセージを送る
        if message.content.lower() == "!ドグ呪文":
            send_message = "⚠️ このコマンドでドグドラの呪文で受けるダメージを計算できます\n"
            send_message += "🎓️ 使い方: \n"
            send_message += "!ドグ呪文 防具(呪文耐性) 料理(炎耐性) 軽減(軽減値)\n"
            send_message += "のようにコメントを書くとダメージ計算結果が表示されます\n"
            send_message += "軽減部分を省略した場合は打たれ名人と職業スキルの22軽減のみで計算します\n"
            send_message += "例: !ドグ呪文 防具(呪14,炎14) 料理(炎14) 軽減(57)\n"
            send_message += "この場合、防具に呪文耐性14%と炎耐性14%、料理で炎耐性14%(☆2)、軽減57を持っていると仮定して計算します\n"
            send_message += "赤竜を装備している場合は軽減(57)と書く必要があります\n\n"
            send_message += "⚠️ 以下の前提でダメージ計算を行います。\n"
            send_message += "※ 宝珠: 呪文耐性+6%, 炎耐性+6%, 風耐性+6%, 打たれ名人+6, やいばの防御+6\n"
            send_message += "※ 職業スキル: ダメージ軽減10あり\n"
            send_message += "※ アクセ: 炎光の勾玉 炎耐性+29%\n"
            send_message += "※ 神速メラガイアー: 1860×3、太古に封じられしバギ: 2000×2 で計算しています。\n"
            send_message += "※ ダメージは平均値なので注意してください。\n"
            await message.channel.send(send_message)
            return
        try:
            send_message = create_spell_damage_message_dogu(message.content)
            await message.channel.send(send_message)
        except Exception as e:
            await message.channel.send(f"⚠️ エラーが発生しました: `{e}`")
    elif message.content.lower().startswith("!ドグ攻撃"):
        # helpの場合は使い方のメッセージを送る
        if message.content.lower() == "!ドグ攻撃":
            send_message = "⚠️ このコマンドでドグドラへのダメージ計算ができます\n"
            send_message += "⚠️ 高速周回を想定した計算になっています。\n"
            send_message += "⚠️ ドグドラのHPは20万です。ばっちり削りきれるように調整してみてください！\n"
            send_message += "🎓️ 使い方:\n"
            send_message += "!ドグ攻撃 職(職業名) 攻撃(攻撃力) 光(光ダメージアップ値) 竜(竜系ダメージアップ値)\n"
            send_message += "のようにコメントを書くとダメージ計算結果が表示されます\n"
            send_message += "追加で以下の条件も指定できます。\n指定しない場合はアヌビスと災禍は無効、牙神は有効、他は全パターン網羅します\n"
            send_message += "・有効(◯◯,✕✕,△△)\n    カッコの中にはFB,ルカニ,DS,レボル,災禍,アヌビスを入れれます\n"
            send_message += "・無効(◯◯,✕✕,△△)\n    カッコの中にはFB,ルカニ,DS,レボル,災禍,アヌビスを入れれます\n"
            send_message += "・失敗(◯◯,✕✕,△△)\n    カッコの中にはFB,ルカニを入れれます\n"
            send_message += "例:\n"
            send_message += "    !ドグ攻撃 職(盗) 攻撃(900) 光(9) 竜(21) 有効(ルカニ,DS) 無効(レボル) 失敗(FB)\n"
            send_message += "    !ドグ攻撃 職(マセン) 攻撃(1000) 竜(9) 有効(レボ,ダークネスショット,災禍、アヌビス、FB)\n"
            send_message += "    !ドグ攻撃 職(レン) 攻撃(700) 光(9) 竜(24) 失敗(FB,ルカニ)\n"
            send_message += "⚠️ 計算時の条件\n"
            send_message += "●ちからはレベル133時点の理論値で計算。盗:450、レン441、魔法448\n"
            send_message += "●武器は爪:機殴竜殺、弓:イルミンズールの弓で計算\n"
            send_message += "●バイキルトあり\n"
            send_message += "●極意系宝珠は最大値で計算\n"
            send_message += "●職業スキルは以下で計算\n"
            send_message += "    特技ダメージアップ+10\n"
            send_message += "    ライガークラッシュ+5%\n"
            send_message += "    シャイニングボウ+5%\n"
            send_message += "    サンライトアロー+2%\n"
            send_message += "●レボルとダクショはそれぞれ強制的にレボルと光耐性ダウンを無効にして表示されます。(打つ前にかけることは出来ないので)\n"
            send_message += "●ダークネスショット用の闇特攻は計算していません。すみません🙏\n"
            await message.channel.send(send_message)
            return
        try:
            send_message_list = create_attack_damage_message_dogu(
                message.content)
            for send_message in send_message_list:
                await message.channel.send(send_message)
        except Exception as e:
            await message.channel.send(f"⚠️ エラーが発生しました: `{e}`")
    elif message.content.lower().startswith(("!バジャ闇", "!baja", "!ばじゃやみ")):
        # helpの場合は使い方のメッセージを送る
        parts = message.content.split()
        if len(parts) == 1:
            send_message = "⚠️ このコマンドでバジャの闇耐性を計算できます\n"
            send_message += "🎓️ 使い方: \n"
            send_message += "!バジャ闇 防具(呪文耐性) 料理(闇耐性) 軽減(軽減値)\n"
            send_message += "のようにコメントを書くとダメージ計算結果が表示されます"
            send_message += "軽減部分を省略した場合は打たれ名人と職業スキルの22軽減のみで計算します\n"
            send_message += "例: !バジャ闇 防具(闇28,ブレス6,呪7) 料理(闇14) 軽減(57)\n"
            send_message += "この場合、防具に闇耐性28%とブレス耐性6%、呪文耐性7%、料理で闇耐性14%(☆2)、軽減57を持っていると仮定して計算します\n"
            send_message += "赤竜を装備している場合は軽減(57)と書く必要があります\n\n"
            send_message += "⚠️ 以下の前提でダメージ計算を行います。\n"
            send_message += "※ 宝珠: 呪文耐性+6%, ブレス耐性+6%, 闇耐性+6%, 打たれ名人+6\n"
            send_message += "※ 職業スキル: ダメージ軽減10あり\n"
            send_message += "※ アクセ: 氷闇の月飾り 闇耐性+29%\n"
            send_message += "※ 闇の流星: 850、ダークネスブレス: 1000、連続ドルマドン: 1100×3 で計算しています。\n"
            send_message += "※ ダメージの最大値は基礎値に+6.25%の補正をかけています。\n"
            await message.channel.send(send_message)
        else:
            try:
                send_message = create_dark_damage_message_baja(message.content)
                await message.channel.send(send_message)
            except Exception as e:
                await message.channel.send(f"⚠️ エラーが発生しました: `{e}`")

# Bot の起動
bot.run(DISCORD_TOKEN)
