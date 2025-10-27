from bedrock_agentcore import BedrockAgentCoreApp
from strands import Agent
from strands_tools import swarm
import json
import re
import asyncio

app = BedrockAgentCoreApp()

def extract_json(message):
    """メッセージからJSON部分を抽出"""
    if isinstance(message, dict):
        if 'content' in message and isinstance(message['content'], list):
            text = message['content'][0].get('text', '')
        else:
            text = str(message)
    else:
        text = str(message)
    
    json_match = re.search(r'```json\s*({.*?})\s*```', text, re.DOTALL)
    if json_match:
        return json.loads(json_match.group(1))
    
    try:
        return json.loads(text)
    except:
        return None

async def invoke_async_streaming(payload):
    """マルチエージェント施策システム（拡張版・ストリーミング対応）"""
    try:
        user_message = payload.get("prompt", "")
        
        if not user_message:
            yield {"type": "error", "data": "プロンプトが必要です"}
            return
        
        # ステップ0: 類似施策の調査
        yield {"type": "status", "data": "[ステップ0] 他自治体の類似施策を調査中..."}
        
        research_agent = Agent(
            model="us.anthropic.claude-sonnet-4-20250514-v1:0",
            callback_handler=None,
            system_prompt="""あなたは自治体施策の調査専門家です。
市民意見に関連する既存の施策事例を調査し、参考になる事例を提示してください。

調査優先順位:
1. 大阪市の事例を最優先
2. 大阪市に事例がなければ他の政令指定都市や大阪府内の市区町村
3. それでもなければ日本全国の自治体事例

出力形式:
```json
{
  "similar_policies": [
    {"municipality": "自治体名", "policy_name": "施策名", "summary": "概要", "results": "成果"}
  ],
  "has_references": true/false,
  "search_scope": "大阪市/他の市区町村/日本全体"
}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。
"""
        )
        
        research_response = ""
        async for event in research_agent.stream_async(f"市民意見: {user_message}\n\nまず大阪市の類似施策事例を調査してください。大阪市に事例がなければ他の市区町村や日本全国の事例を3つ程度調査してください。"):
            if "data" in event:
                chunk = event["data"]
                yield {"type": "stream", "step": "research", "data": chunk}
                research_response += chunk
        
        research_result = extract_json(research_response) or {"similar_policies": [], "has_references": False}
        yield {"type": "research", "data": research_result}
        yield {"type": "stream", "step": "research_complete", "data": f"\n\n【調査完了】類似施策: {len(research_result.get('similar_policies', []))}件"}
        
        # ステップ1a: 人口動態調査
        yield {"type": "status", "data": "[ステップ1a] 対象地域の人口動態を調査中..."}
        
        demographics_agent = Agent(
            model="us.anthropic.claude-sonnet-4-20250514-v1:0",
            callback_handler=None,
            system_prompt="""あなたは人口統計の専門家です。
市民意見から対象地域を特定し、その地域の人口動態を調査してください。

調査優先順位:
1. 大阪市の人口動態を最優先
2. 市民意見で特定の地域が明示されている場合はその地域
3. 大阪市のデータが不明な場合は他の政令指定都市や日本全体の統計

重要: データが存在しない場合は、フェルミ推定を使用してください。
- 類似都市のデータから類推
- 日本全体の統計から地域特性を考慮して補正
- 人口規模、産業構造、地理的特性から推定
- 推定方法を必ずdata_sourceに明記すること

日本語習熟度の判断基準（外国人住民の場合）:
- fluent (流暢): JLPT N1-N2相当。行政文書の読解、窓口での複雑な相談、就労に支障なし
- conversational (会話可能): JLPT N3-N4相当。日常会話は可能だが、専門用語や書類手続きに支援が必要
- basic (基礎レベル): JLPT N5相当またはそれ以下。挨拶・簡単な買い物程度。生活全般で支援が必要
- needs_support (支援必須): ほぼ日本語不可。通訳・翻訳が常時必要

出力形式:
```json
{
  "target_area": "対象地域名",
  "age_distribution": {
    "20代": 10,
    "30代": 15,
    "40代": 15,
    "50代": 20,
    "60代以上": 40
  },
  "gender_ratio": {"male": 48, "female": 52},
  "family_types": [
    {"type": "単身世帯", "percentage": 35},
    {"type": "夫婦のみ", "percentage": 20},
    {"type": "子育て世帯", "percentage": 25},
    {"type": "三世代同居", "percentage": 10},
    {"type": "高齢者のみ", "percentage": 10}
  ],
  "language_distribution": [
    {"language": "日本語", "percentage": 60, "notes": "備考"},
    {"language": "英語", "percentage": 15, "notes": "主にビジネス層"}
  ],
  "japanese_proficiency_levels": {
    "fluent": 30,
    "conversational": 40,
    "basic": 20,
    "needs_support": 10
  },
  "cultural_considerations": [
    {"group": "地域・文化圏", "key_points": ["宗教行事の配慮", "学校での文化摩擦"]},
    {"group": "技能実習生", "key_points": ["行政手続きの支援", "労働時間管理"]}
  ],
  "priority_services": [
    "行政手続きの多言語化（日本語・英語・中国語・ベトナム語）",
    "学校での多文化サポート教員の配置"
  ],
  "data_source": "データソース（文字列で記載。例: 大阪市統計書2023年版、総務省統計局2022年国勢調査、フェルミ推定による）",
  "data_scope": "大阪市/他の市区町村/日本全体"
}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

注意: data_sourceは必ず文字列で記載してください。オブジェクトや配列は使用しないでください。"""
        )
        
        demographics_response = ""
        async for event in demographics_agent.stream_async(f"市民意見: {user_message}\n\nまず大阪市の人口動態を調査してください。大阪市のデータが不明な場合は他の市区町村や日本全体の統計を使用してください。\n\nデータが存在しない場合は、フェルミ推定で合理的な推定値を算出してください。推定方法をdata_sourceに明記してください。"):
            if "data" in event:
                chunk = event["data"]
                yield {"type": "stream", "step": "demographics", "data": chunk}
                demographics_response += chunk
        
        demographics_data = extract_json(demographics_response)
        if not demographics_data:
            # Retry up to 3 times on JSON parse failure
            for retry_attempt in range(1, 4):
                yield {"type": "status", "data": f"[ステップ1a] 人口動態データのJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                demographics_response = ""
                async for event in demographics_agent.stream_async(f"市民意見: {user_message}\n\nまず大阪市の人口動態を調査してください。大阪市のデータが不明な場合は他の市区町村や日本全体の統計を使用してください。\n\nデータが存在しない場合は、フェルミ推定で合理的な推定値を算出してください。推定方法をdata_sourceに明記してください。"):
                    if "data" in event:
                        chunk = event["data"]
                        yield {"type": "stream", "step": f"demographics_retry_{retry_attempt}", "data": chunk}
                        demographics_response += chunk
                demographics_data = extract_json(demographics_response)
                if demographics_data:
                    break
        if not demographics_data:
            yield {"type": "error", "data": "人口動態データの取得に失敗しました"}
            return
        yield {"type": "demographics", "data": demographics_data}
        language_distribution = demographics_data.get('language_distribution', [])
        language_summary = ", ".join(
            f"{entry.get('language', '不明')}: {entry.get('percentage', '?')}%"
            for entry in language_distribution[:3]
        ) or "不明"
        japanese_proficiency = demographics_data.get('japanese_proficiency_levels', {})
        proficiency_summary = ", ".join(
            f"{level}: {percentage}%"
            for level, percentage in japanese_proficiency.items()
        ) or "不明"
        yield {"type": "stream", "step": "demographics_complete", "data": (
            f"\n\n【調査完了】対象地域: {demographics_data.get('target_area', '不明')}"
            f"\n年齢分布: {json.dumps(demographics_data.get('age_distribution', {}), ensure_ascii=False)}"
            f"\n性別比率: {json.dumps(demographics_data.get('gender_ratio', {}), ensure_ascii=False)}"
            f"\n主な言語: {language_summary}"
            f"\n日本語習熟度: {proficiency_summary}"
        )}
        
        # ステップ1b: SVエージェントがエージェント定義を生成（調査した人口動態に基づく）
        yield {"type": "status", "data": "[ステップ1b] エージェント定義を生成中..."}
        
        demographics_text = f"""
対象地域: {demographics_data.get('target_area', '不明')}
年齢分布: {json.dumps(demographics_data.get('age_distribution', {}), ensure_ascii=False)}
性別比率: {json.dumps(demographics_data.get('gender_ratio', {}), ensure_ascii=False)}
家族構成: {json.dumps(demographics_data.get('family_types', []), ensure_ascii=False)}
言語分布: {json.dumps(demographics_data.get('language_distribution', []), ensure_ascii=False)}
日本語習熟度: {json.dumps(demographics_data.get('japanese_proficiency_levels', {}), ensure_ascii=False)}
文化的配慮事項: {json.dumps(demographics_data.get('cultural_considerations', []), ensure_ascii=False)}
優先サービス: {json.dumps(demographics_data.get('priority_services', []), ensure_ascii=False)}
"""
        
        sv_agent = Agent(
            model="us.anthropic.claude-sonnet-4-20250514-v1:0",
            callback_handler=None,
            system_prompt="""市民意見を分析し、施策検討に必要なエージェントを設計してください。

あなたの役割:
1. 市民意見の内容を分析
2. 必要な施策立案エージェントの数と専門分野を決定（目安: 2-4名）
   - 必ず大阪市行政の視点を持つエージェントを1名以上含める
   - ただし、name欄には「大阪市の」を付けず、一般的な職種名や専門分野名のみ記載
3. 市民評価エージェントを最低10名設定（提供された人口動態データに基づく）

施策立案エージェントのエージェント名ルール:
- 良い例: "施策企画担当者", "福祉施策専門家", "都市計画コンサルタント", "DX推進専門家"
- 悪い例: "大阪市の福祉局担当者", "大阪市の都市整備局職員" （具体的な部署名は避ける）

市民エージェント設計ルール（仮想市民エージェント設計士として）:
目的：施策内容をもとに、施策レポーティングで多様な意見を生む10体の仮想市民を設計する。

設計ルール:
- 人口動態を参考に、全年齢・全層からバランスよく構成する
- 施策の主要対象層を30〜50％程度含める
- 間接的に関わる層や施策非対象層も含める
- 外国人、高齢者、障がい者、子育て世帯など多様な背景を持つ市民を適度に含める
- ステレオタイプを避け、現実的な背景・意見を設計する
- 施策への立場（賛成／中立／懸念など）は均等に分布させる

出力形式:
```json
{
  "policy_agents": [
    {"name": "施策企画担当者", "expertise": "施策立案・行政実務", "system_prompt": "詳細なプロンプト"}
  ],
  "citizen_agents": [
    {
      "name": "田中花子",
      "age": 30,
      "gender": "女性",
      "occupation": "保育士",
      "residence": "大阪市東成区",
      "family": "共働き・子2人",
      "values": "地域とのつながりを重視",
      "stance": "強く賛成",
      "profile": "詳細なプロフィール",
      "is_directly_affected": true,
      "system_prompt": "評価用プロンプト"
    }
  ],
  "reviewer_agent": {
    "name": "法務・実現性レビュアー",
    "expertise": "法律・実現可能性",
    "system_prompt": "レビュー用プロンプト"
  }
}
```

厳守事項:
- JSON項目名は英語、値は日本語で記載してください。
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

注意: 
- 施策立案エージェントの1名以上は必ず大阪市行政の立場で考える専門家とする
- しかし、name欄には「大阪市の」を含めず、一般的な職種名のみ記載する
- system_promptでは「大阪市の立場から」など具体的な視点を明記する
- is_directly_affected は施策の直接的な恩恵を受けるかどうかを示します（true=恩恵を受ける、false=恩恵を受けない/関係ない層）
- 市民エージェントのJSON項目は全て英語で記載すること"""
        )
        
        sv_response = ""
        async for event in sv_agent.stream_async(f"市民意見: {user_message}\n\n人口動態データ:\n{demographics_text}"):
            if "data" in event:
                chunk = event["data"]
                yield {"type": "stream", "step": "sv_agent", "data": chunk}
                sv_response += chunk
        
        agent_defs = extract_json(sv_response)
        if not agent_defs:
            # Retry up to 3 times on JSON parse failure
            for retry_attempt in range(1, 4):
                yield {"type": "status", "data": f"[ステップ1b] エージェント定義のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                sv_response = ""
                async for event in sv_agent.stream_async(f"市民意見: {user_message}\n\n人口動態データ:\n{demographics_text}"):
                    if "data" in event:
                        chunk = event["data"]
                        yield {"type": "stream", "step": f"sv_agent_retry_{retry_attempt}", "data": chunk}
                        sv_response += chunk
                agent_defs = extract_json(sv_response)
                if agent_defs:
                    break
        
        if not agent_defs or len(agent_defs.get("citizen_agents", [])) < 10:
            yield {"type": "error", "data": "エージェント定義の生成に失敗しました（市民エージェントが10名未満）"}
            return
        
        # is_directly_affectedフィールドの確認と警告
        unaffected_count = sum(1 for a in agent_defs.get("citizen_agents", []) if a.get("is_directly_affected") == False)
        yield {"type": "status", "data": f"[ステップ1b] 生成完了: 市民エージェント{len(agent_defs.get('citizen_agents', []))}名（うち施策対象外{unaffected_count}名）"}
        
        yield {"type": "agent_defs", "data": agent_defs}
        
        # ステップ2: Swarmで施策立案（類似施策を参考に）
        yield {"type": "status", "data": "[ステップ2] 施策立案エージェントが協調実行中..."}
        
        reference_text = ""
        if research_result.get("has_references"):
            reference_text = f"\n\n参考事例:\n{json.dumps(research_result['similar_policies'], ensure_ascii=False, indent=2)}\n上記事例を参考にしてください。"
        
        swarm_agent = Agent(
            model="us.anthropic.claude-sonnet-4-20250514-v1:0",
            tools=[swarm],
            callback_handler=None
        )
        
        swarm_prompt = f"""以下のエージェント定義に基づいてswarmを作成し、市民意見「{user_message}」に対する施策案をJSON形式で作成してください。

エージェント定義:
{json.dumps(agent_defs['policy_agents'], ensure_ascii=False, indent=2)}
{reference_text}

出力形式:
```json
{{
  "policy_title": "施策名（簡潔で分かりやすいタイトル、50文字以内）",
  "summary": "施策概要（この施策が何を目的とし、誰を対象とするかを簡潔に説明、300-500文字）",
  "referenced_policies": ["参考にした自治体施策名（具体的な自治体名と施策名）"],
  "problem_analysis": "問題分析（現状の課題、なぜこの施策が必要なのか、データや具体例を含めて説明、500-700文字）",
  "detailed_policy": "施策詳細（具体的な施策内容、支援内容、対象者の条件、実施方法、予算規模の目安、必要な体制、考慮すべき事項（法律、既存施策との関係など）を詳しく記載、800-1000文字）",
  "implementation_plan": "実施計画（どれくらいの期間でどのように進めていくか、各フェーズの期間と内容、段階的な展開方法を記載、500-700文字）",
  "expected_effects": "期待効果（定量的効果（例：年間○○人が利用、○○%改善）と定性的効果（例：市民満足度向上、地域活性化）を具体的に記載、400-600文字）",
  "is_temporary": true/false（一時的な施策ならtrue、恒久的な施策ならfalse）
}}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

**必須事項**:
- 上記の全ての項目を必ず含めてください
- 各項目の説明に従って、具体的かつ詳細に記載してください
- 文字数目安を参考に、十分な情報量を確保してください"""
        
        policy_response = ""
        async for event in swarm_agent.stream_async(swarm_prompt):
            if "data" in event:
                chunk = event["data"]
                yield {"type": "stream", "step": "swarm", "data": chunk}
                policy_response += chunk
        
        policy_json = extract_json(policy_response)
        if not policy_json:
            # Retry up to 3 times on JSON parse failure
            for retry_attempt in range(1, 4):
                yield {"type": "status", "data": f"[ステップ2] 施策案のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                policy_response = ""
                async for event in swarm_agent.stream_async(swarm_prompt):
                    if "data" in event:
                        chunk = event["data"]
                        yield {"type": "stream", "step": f"swarm_retry_{retry_attempt}", "data": chunk}
                        policy_response += chunk
                policy_json = extract_json(policy_response)
                if policy_json:
                    break
        if not policy_json:
            policy_json = {"raw_text": policy_response}
        
        yield {"type": "policy", "data": policy_json}
        
        # ステップ3: レビュアーによる法律・実現性チェック（最大3回再試行）
        yield {"type": "status", "data": "[ステップ3] レビュアーが法律・実現性をチェック中..."}
        
        reviewer_agent = Agent(
            model="us.anthropic.claude-sonnet-4-20250514-v1:0",
            system_prompt=agent_defs.get("reviewer_agent", {}).get("system_prompt", "法律と実現性の観点でレビューしてください"),
            callback_handler=None
        )
        
        review_result = None
        for attempt in range(1, 4):
            yield {"type": "status", "data": f"[ステップ3] レビュー試行 {attempt}/3"}
            
            review_prompt = f"""以下の施策案を法律と実現性の観点でレビューしてください。

施策案:
{json.dumps(policy_json, ensure_ascii=False, indent=2)}

出力形式:
```json
{{
  "legal_compliance": {{"score": 85, "issues": ["問題点"], "recommendations": ["推奨事項"]}},
  "feasibility": {{"score": 80, "issues": ["問題点"], "recommendations": ["推奨事項"]}},
  "total_score": 82.5,
  "overall_assessment": "総合評価",
  "approved": true/false,
  "improvement_suggestions": "改善提案（承認されない場合）"
}}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

総合スコア = 法令適合性×0.5 + 実現可能性×0.5
承認基準: 80点以上で承認

重要: overall_assessmentとimprovement_suggestionsは、【】で見出しを付け、箇条書き『・』を使用して読みやすく記載してください。
"""
            
            review_response = ""
            async for event in reviewer_agent.stream_async(review_prompt):
                if "data" in event:
                    chunk = event["data"]
                    yield {"type": "stream", "step": f"reviewer_attempt_{attempt}", "data": chunk}
                    review_response += chunk
            
            review_result = extract_json(review_response)
            if not review_result:
                # Retry up to 3 times on JSON parse failure
                for retry_attempt in range(1, 4):
                    yield {"type": "status", "data": f"[ステップ3] レビュー応答のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                    review_response = ""
                    async for event in reviewer_agent.stream_async(review_prompt):
                        if "data" in event:
                            chunk = event["data"]
                            yield {"type": "stream", "step": f"reviewer_attempt_{attempt}_retry_{retry_attempt}", "data": chunk}
                            review_response += chunk
                    review_result = extract_json(review_response)
                    if review_result:
                        break
            review_result = review_result or {"approved": False, "total_score": 0}
            
            # 総合スコアを計算（法令適合性50% + 実現可能性50%）
            if "total_score" not in review_result:
                legal_score = review_result.get("legal_compliance", {}).get("score", 0)
                feasibility_score = review_result.get("feasibility", {}).get("score", 0)
                review_result["total_score"] = legal_score * 0.5 + feasibility_score * 0.5
            
            # 80点以上で承認
            review_result["approved"] = review_result["total_score"] >= 80
            yield {"type": "review", "data": {**review_result, "attempt": attempt}}
            
            if review_result.get("approved", False):
                yield {"type": "status", "data": f"[ステップ3] レビュー承認（{attempt}回目）"}
                break
            
            if attempt < 3:
                yield {"type": "status", "data": f"[ステップ3] 承認されず、施策案を改善中..."}
                
                # 施策案を改善
                improvement_prompt = f"""以下の施策案がレビューで承認されませんでした。

元の施策案:
{json.dumps(policy_json, ensure_ascii=False, indent=2)}

レビュー結果:
{json.dumps(review_result, ensure_ascii=False, indent=2)}

改善提案に基づいて施策案を修正してください。出力形式は元の施策案と同じJSON形式です。"""
                
                policy_response = ""
                async for event in swarm_agent.stream_async(improvement_prompt):
                    if "data" in event:
                        chunk = event["data"]
                        yield {"type": "stream", "step": f"improvement_{attempt}", "data": chunk}
                        policy_response += chunk
                
                improved_policy = extract_json(policy_response)
                if not improved_policy:
                    # Retry up to 3 times on JSON parse failure
                    for retry_attempt in range(1, 4):
                        yield {"type": "status", "data": f"[ステップ3] 改善施策案のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                        policy_response = ""
                        async for event in swarm_agent.stream_async(improvement_prompt):
                            if "data" in event:
                                chunk = event["data"]
                                yield {"type": "stream", "step": f"improvement_{attempt}_retry_{retry_attempt}", "data": chunk}
                                policy_response += chunk
                        improved_policy = extract_json(policy_response)
                        if improved_policy:
                            break
                if improved_policy:
                    policy_json = improved_policy
                    yield {"type": "policy", "data": {**policy_json, "improved": True, "attempt": attempt}}
            else:
                yield {"type": "status", "data": "[ステップ3] 3回目も承認されませんでしたが、処理を続行します"}
        
        yield {"type": "review_final", "data": review_result}
        
        # ステップ4: 市民評価（濃い評価）
        yield {"type": "status", "data": "[ステップ4] 市民エージェントが評価中..."}
        
        policy_summary = f"""
施策名: {policy_json.get('policy_title', 'N/A')}
施策概要: {policy_json.get('summary', 'N/A')}
問題分析: {policy_json.get('problem_analysis', 'N/A')}
施策詳細: {policy_json.get('detailed_policy', 'N/A')}
実施計画: {policy_json.get('implementation_plan', 'N/A')}
期待効果: {policy_json.get('expected_effects', 'N/A')}
参考事例: {', '.join(policy_json.get('referenced_policies', []))}
"""
        
        citizen_evaluations = []
        total_citizens = len(agent_defs["citizen_agents"])
        
        # 重要: 全市民エージェントが必ず評価を実施するように、各エージェントを順番に処理
        for i, agent_def in enumerate(agent_defs["citizen_agents"]):
            yield {"type": "status", "data": f"[ステップ4] 市民評価 {i+1}/{total_citizens}: {agent_def['name']}"}
            
            citizen_agent = Agent(
                model="us.anthropic.claude-sonnet-4-20250514-v1:0",
                system_prompt=agent_def["system_prompt"],
                callback_handler=None
            )
            
            eval_prompt = f"""{policy_summary}

あなたの立場: {agent_def['profile']}
年齢: {agent_def['age']}歳、性別: {agent_def.get('gender', '')}、家族: {agent_def.get('family', '')}

重要: あなたは市民エージェント{i+1}番目（全{total_citizens}名中）です。必ず評価を完了してください。

上記の施策案を、以下の5つの観点から100点満点で評価してください。
各項目には点数とコメント（具体的な理由や影響の説明）を記載してください。

出力形式:
```json
{{
  "evaluator_name": "{agent_def['name']}",
  "age": {agent_def['age']},
  "gender": "{agent_def.get('gender', '')}",
  "occupation": "{agent_def.get('occupation', '')}",
  "residence": "{agent_def.get('residence', '')}",
  "family": "{agent_def.get('family', '')}",
  "values": "{agent_def.get('values', '')}",
  "stance": "{agent_def.get('stance', '')}",
  "personal_impact": {{"score": 75, "comment": "この施策が自分の生活にどう影響するか（具体的に150文字程度）"}},
  "family_impact": {{"score": 80, "comment": "この施策が家族にどう影響するか（具体的に150文字程度）"}},
  "community_impact": {{"score": 70, "comment": "この施策が地域にどう影響するか（具体的に150文字程度）"}},
  "fairness": {{"score": 65, "comment": "この施策の公平性についての評価（具体的に150文字程度）"}},
  "sustainability": {{"score": 60, "comment": "この施策の持続可能性についての評価（具体的に150文字程度）"}},
  "overall_rating": 72.5,
  "expectations": "期待すること（具体的に100文字程度）",
  "concerns": "懸念すること（具体的に100文字程度）",
  "recommendations": "提言（具体的に100文字程度）"
}}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

必須: 上記の全ての項目を必ず出力してください。あなたは{total_citizens}名の市民エージェントの1人として、必ず評価を完了する責任があります。

総合評価 = 個人影響×0.5 + 家族影響×0.2 + 地域影響×0.1 + 公平性×0.1 + 持続可能性×0.1
"""
            
            try:
                eval_response = ""
                async for event in citizen_agent.stream_async(eval_prompt):
                    if "data" in event:
                        chunk = event["data"]
                        yield {"type": "stream", "step": f"citizen_{i}", "data": chunk}
                        eval_response += chunk
                
                evaluation = extract_json(eval_response)
                if not evaluation:
                    # Retry up to 3 times on JSON parse failure
                    for retry_attempt in range(1, 4):
                        yield {"type": "status", "data": f"[ステップ4] 市民評価のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                        eval_response = ""
                        async for event in citizen_agent.stream_async(eval_prompt):
                            if "data" in event:
                                chunk = event["data"]
                                yield {"type": "stream", "step": f"citizen_{i}_retry_{retry_attempt}", "data": chunk}
                                eval_response += chunk
                        evaluation = extract_json(eval_response)
                        if evaluation:
                            break
                if evaluation:
                    evaluation["is_directly_affected"] = agent_def.get("is_directly_affected", True)
                    citizen_evaluations.append(evaluation)
                    yield {"type": "evaluation", "data": evaluation}
                    yield {"type": "status", "data": f"[ステップ4] ✅ {agent_def['name']}の評価完了 ({i+1}/{total_citizens})"}
                else:
                    error_eval = {"evaluator_name": agent_def['name'], "error": "JSON解析失敗", "is_directly_affected": agent_def.get("is_directly_affected", True)}
                    citizen_evaluations.append(error_eval)
                    yield {"type": "evaluation", "data": error_eval}
                    yield {"type": "status", "data": f"[ステップ4] ⚠️ {agent_def['name']}の評価でエラー ({i+1}/{total_citizens})"}
            except Exception as e:
                error_eval = {"evaluator_name": agent_def['name'], "error": str(e), "is_directly_affected": agent_def.get("is_directly_affected", True)}
                citizen_evaluations.append(error_eval)
                yield {"type": "evaluation", "data": error_eval}
                yield {"type": "status", "data": f"[ステップ4] ❌ {agent_def['name']}の評価で例外発生 ({i+1}/{total_citizens}): {str(e)}"}
        
        yield {"type": "status", "data": f"[ステップ4] 市民評価完了: {len(citizen_evaluations)}/{total_citizens}名"}
        
        # ステップ5: 10年後評価（一時的施策でない場合）
        future_evaluations = []
        if not policy_json.get("is_temporary", False):
            yield {"type": "status", "data": "[ステップ5] 10年後の評価をシミュレーション中..."}
            
            # 重要: 全市民エージェントが必ず10年後評価を実施するように、各エージェントを順番に処理
            for i, agent_def in enumerate(agent_defs["citizen_agents"]):
                yield {"type": "status", "data": f"[ステップ5] 10年後評価 {i+1}/{total_citizens}: {agent_def['name']}"}
                
                citizen_agent = Agent(
                    model="us.anthropic.claude-sonnet-4-20250514-v1:0",
                    system_prompt=agent_def["system_prompt"],
                    callback_handler=None
                )
                
                current_family = agent_def.get('family', '')
                future_family_note = ""
                if current_family:
                    future_family_note = f"\n\n現在の家族構成: {current_family}\n10年後の家族構成を推測してください（例: 子供が成人、独立、結婚など）。現在の年齢や状況から自然な変化を想定してください。"
                
                future_prompt = f"""{policy_summary}

あなたは10年後の{agent_def['age']+10}歳になっています。
この施策が実施されて10年が経過しました。{future_family_note}

重要: あなたは市民エージェント{i+1}番目（全{total_citizens}名中）です。必ず10年後評価を完了してください。

10年間の変化と現在の評価を述べてください。

出力形式:
```json
{{
  "evaluator_name": "{agent_def['name']} (10年後)",
  "age_now": {agent_def['age']+10},
  "ten_year_rating": 75,
  "changes_observed": "10年間で観察された変化（家族構成の変化も含む）",
  "long_term_impact": "長期的な影響の評価",
  "unexpected_outcomes": "予想外の結果",
  "current_opinion": "現在の意見"
}}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

必須: 
- ten_year_ratingは100点満点で評価してください。
- changes_observedには、現在の家族構成から10年後の自然な変化（子供の成長、独立など）を含めてください。
- あなたは{total_citizens}名の市民エージェントの1人として、必ず10年後評価を完了する責任があります。
"""
                
                try:
                    future_response = ""
                    async for event in citizen_agent.stream_async(future_prompt):
                        if "data" in event:
                            chunk = event["data"]
                            yield {"type": "stream", "step": f"future_{i}", "data": chunk}
                            future_response += chunk
                    
                    future_eval = extract_json(future_response)
                    if not future_eval:
                        # Retry up to 3 times on JSON parse failure
                        for retry_attempt in range(1, 4):
                            yield {"type": "status", "data": f"[ステップ5] 10年後評価のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                            future_response = ""
                            async for event in citizen_agent.stream_async(future_prompt):
                                if "data" in event:
                                    chunk = event["data"]
                                    yield {"type": "stream", "step": f"future_{i}_retry_{retry_attempt}", "data": chunk}
                                    future_response += chunk
                            future_eval = extract_json(future_response)
                            if future_eval:
                                break
                    if future_eval:
                        future_evaluations.append(future_eval)
                        yield {"type": "future_evaluation", "data": future_eval}
                        yield {"type": "status", "data": f"[ステップ5] ✅ {agent_def['name']}の10年後評価完了 ({i+1}/{total_citizens})"}
                    else:
                        error_eval = {"evaluator_name": f"{agent_def['name']} (10年後)", "error": "JSON解析失敗"}
                        future_evaluations.append(error_eval)
                        yield {"type": "future_evaluation", "data": error_eval}
                        yield {"type": "status", "data": f"[ステップ5] ⚠️ {agent_def['name']}の10年後評価でエラー ({i+1}/{total_citizens})"}
                except Exception as e:
                    error_eval = {"evaluator_name": f"{agent_def['name']} (10年後)", "error": str(e)}
                    future_evaluations.append(error_eval)
                    yield {"type": "future_evaluation", "data": error_eval}
                    yield {"type": "status", "data": f"[ステップ5] ❌ {agent_def['name']}の10年後評価で例外発生 ({i+1}/{total_citizens}): {str(e)}"}
            
            yield {"type": "status", "data": f"[ステップ5] 10年後評価完了: {len(future_evaluations)}/{total_citizens}名"}
        
        # ステップ6: 最終評価
        yield {"type": "status", "data": "[ステップ6] 最終評価を算出中..."}
        
        # 市民評価から各指標を集計
        citizen_personal = [e.get("personal_impact", {}).get("score", 0) for e in citizen_evaluations if "personal_impact" in e]
        citizen_family = [e.get("family_impact", {}).get("score", 0) for e in citizen_evaluations if "family_impact" in e]
        citizen_community = [e.get("community_impact", {}).get("score", 0) for e in citizen_evaluations if "community_impact" in e]
        citizen_fairness = [e.get("fairness", {}).get("score", 0) for e in citizen_evaluations if "fairness" in e]
        citizen_sustainability = [e.get("sustainability", {}).get("score", 0) for e in citizen_evaluations if "sustainability" in e]
        
        # 効果・成果スコア（市民評価をそのまま反映）
        effectiveness_personal = (sum(citizen_personal) / len(citizen_personal)) if citizen_personal else 50
        effectiveness_family = (sum(citizen_family) / len(citizen_family)) if citizen_family else 50
        effectiveness_community = (sum(citizen_community) / len(citizen_community)) if citizen_community else 50
        effectiveness_score = effectiveness_personal * 0.5 + effectiveness_family * 0.2 + effectiveness_community * 0.1
        
        # 公平性スコア（市民評価の50%を反映）
        citizen_fairness_avg = (sum(citizen_fairness) / len(citizen_fairness)) if citizen_fairness else 50
        
        # 持続可能性スコア（市民評価の50%を反映）
        citizen_sustainability_avg = (sum(citizen_sustainability) / len(citizen_sustainability)) if citizen_sustainability else 50
        
        final_evaluator = Agent(
            model="us.anthropic.claude-sonnet-4-20250514-v1:0",
            callback_handler=None,
            system_prompt="""あなたは施策評価の専門家です。
以下の5つの観点から施策を評価してください。

1. 透明性・説明責任（Transparency）- 重み20%
2. 社会的受容性・倫理性（Ethical Acceptability）- 重み10%
3. 効果・成果（Effectiveness）- 重み25% （市民評価の個人影響50%、家族影響20%、地域影響10%をそのまま反映）
4. 公平性（Equity）- 重み25% （うち50%は市民評価の公平性を反映）
5. 持続可能性・コスト効率（Sustainability）- 重み15% （うち50%は市民評価の持続可能性を反映）

出力形式:
```json
{{
  "equity": {{"score": 75, "comment": "評価コメント"}},
  "effectiveness": {{"score": 80, "comment": "評価コメント"}},
  "transparency": {{"score": 70, "comment": "評価コメント"}},
  "sustainability": {{"score": 65, "comment": "評価コメント"}},
  "ethical_acceptability": {{"score": 85, "comment": "評価コメント"}},
  "total_score": 75.5,
  "overall_comment": "総合評価コメント",
  "recommendation": "推奨/条件付き推奨/再検討推奨"
}}
```

厳守事項:
- 出力は純粋なJSONオブジェクト1つのみ。前後に`json`や説明文、コメント、Markdownコードブロックを付けないこと。
- 記号はすべて半角で記述し、全角記号（「、」「。」など）は使用しないこと。
- 各キーは1回だけ出力し、余計なコメントや重複キーを含めないこと。

重要: total_scoreは必ず以下の計算式で算出してください：
total_score = equity.score × 0.25 + effectiveness.score × 0.25 + transparency.score × 0.20 + sustainability.score × 0.15 + ethical_acceptability.score × 0.10"""
        )
        
        final_prompt = f"""施策案:
{json.dumps(policy_json, ensure_ascii=False, indent=2)}

市民評価数: {len(citizen_evaluations)}名
市民評価データ:
{json.dumps(citizen_evaluations, ensure_ascii=False, indent=2)}

市民評価からの集計データ:
- 個人影響平均: {effectiveness_personal:.1f}点
- 家族影響平均: {effectiveness_family:.1f}点
- 地域影響平均: {effectiveness_community:.1f}点
- 公平性平均: {citizen_fairness_avg:.1f}点
- 持続可能性平均: {citizen_sustainability_avg:.1f}点

以下の5つの観点で施策を評価してください：

1. 透明性・説明責任（Transparency）- 重み20%
   - 意思決定の根拠や過程が明示されているか
   - 根拠データ数、説明可能性を評価

2. 社会的受容性・倫理性（Ethical Acceptability）- 重み10%
   - 人権・プライバシー・倫理的観点から適切か

3. 効果・成果（Effectiveness）- 重み25%
   - 市民評価をそのまま反映: {effectiveness_score:.1f}点
   - 内訳: 個人影響({effectiveness_personal:.1f})×50% + 家族影響({effectiveness_family:.1f})×20% + 地域影響({effectiveness_community:.1f})×10%
   - このスコアをそのまま使用: {effectiveness_score:.1f}点

4. 公平性（Equity）- 重み25%
   - 市民評価の公平性平均: {citizen_fairness_avg:.1f}点（これが50%を占める）
   - 施策が特定層に偏らず、公平に恩恵が行き渡るか（残り50%）
   - 支援対象分布の偏り、格差是正度を評価

5. 持続可能性・コスト効率（Sustainability）- 重み15%
   - 市民評価の持続可能性平均: {citizen_sustainability_avg:.1f}点（これが50%を占める）
   - 財政的・人的リソース観点から継続可能か（残り50%）
   - コスト対効果比、長期的影響度を評価

総合スコア = 透明性×0.20 + 社会的受容性×0.10 + 効果・成果×0.25 + 公平性×0.25 + 持続可能性×0.15

推奨判定基準:
- 70点以上: 推奨
- 50-69点: 条件付き推奨
- 50点未満: 再検討推奨
"""
        
        final_response = ""
        async for event in final_evaluator.stream_async(final_prompt):
            if "data" in event:
                chunk = event["data"]
                yield {"type": "stream", "step": "final_assessment", "data": chunk}
                final_response += chunk
        
        final_assessment = extract_json(final_response)
        if not final_assessment:
            # Retry up to 3 times on JSON parse failure
            for retry_attempt in range(1, 4):
                yield {"type": "status", "data": f"[ステップ6] 最終評価のJSON解析に失敗。再試行中... ({retry_attempt}/3)"}
                final_response = ""
                async for event in final_evaluator.stream_async(final_prompt):
                    if "data" in event:
                        chunk = event["data"]
                        yield {"type": "stream", "step": f"final_assessment_retry_{retry_attempt}", "data": chunk}
                        final_response += chunk
                final_assessment = extract_json(final_response)
                if final_assessment:
                    break
        final_assessment = final_assessment or {"total_score": 0}
        yield {"type": "final_assessment", "data": final_assessment}
        
        result_json = {
            "status": "success",
            "user_message": user_message,
            "research_result": research_result,
            "demographics_data": demographics_data,
            "generated_agents": {
                "policy_agents": [{"name": a["name"], "expertise": a["expertise"]} for a in agent_defs["policy_agents"]],
                "citizen_agents": [{"name": a["name"], "age": a["age"], "profile": a["profile"], "is_directly_affected": a.get("is_directly_affected", True)} for a in agent_defs["citizen_agents"]],
                "reviewer": agent_defs.get("reviewer_agent", {}).get("name", "レビュアー")
            },
            "policy_proposal": policy_json,
            "review_result": review_result,
            "citizen_evaluations": citizen_evaluations,
            "future_evaluations": future_evaluations,
            "final_assessment": final_assessment,
            "execution_status": {
                "completed": True,
                "policy_agents_count": len(agent_defs["policy_agents"]),
                "citizen_agents_count": len(agent_defs["citizen_agents"]),
                "has_future_evaluation": len(future_evaluations) > 0
            }
        }
        
        yield {"type": "complete", "data": result_json}
    
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        yield {"type": "error", "data": f"エラーが発生しました: {str(e)}"}
        print(f"\n\nエラー詳細:\n{error_msg}")

@app.entrypoint
async def invoke(payload):
    """AgentCore Runtime エントリーポイント（ストリーミング対応）"""
    async for chunk in invoke_async_streaming(payload):
        yield chunk

if __name__ == "__main__":
    # AgentCore Runtimeデプロイ用
    app.run()
