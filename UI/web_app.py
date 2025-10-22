from flask import Flask, render_template, request, jsonify, Response
import json
import boto3
from botocore.config import Config
import os
import uuid

app = Flask(__name__)

AGENT_ARN = os.environ.get('AGENT_ARN', 'arn:aws:bedrock-agentcore:us-west-2:047786098634:runtime/multi_agent_app_enhanced-6tKJtDDS6F')
REGION = os.environ.get('AWS_REGION', 'us-west-2')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/evaluate', methods=['POST'])
def evaluate():
    try:
        data = request.json
        prompt = data.get('prompt', '')
        
        if not prompt:
            return jsonify({'error': 'プロンプトが必要です'}), 400
        
        def generate():
            try:
                config = Config(
                    read_timeout=3600,
                    connect_timeout=60,
                    retries={'max_attempts': 0}
                )
                
                agent_core_client = boto3.client('bedrock-agentcore', region_name=REGION, config=config)
                
                payload = json.dumps({"prompt": prompt}).encode()
                session_id = str(uuid.uuid4()) + str(uuid.uuid4())[:5]
                
                response = agent_core_client.invoke_agent_runtime(
                    agentRuntimeArn=AGENT_ARN,
                    runtimeSessionId=session_id,
                    payload=payload
                )
                
                content_type = response.get('contentType', '')
                
                if "text/event-stream" in content_type:
                    for line in response["response"].iter_lines(chunk_size=10):
                        if line:
                            line = line.decode("utf-8")
                            if line.startswith("data: "):
                                line = line[6:]
                                # そのまま送信（JSONパース不要）
                                yield f"data: {line}\n\n"
                
                elif content_type == "application/json":
                    content = []
                    for chunk in response.get("response", []):
                        content.append(chunk.decode('utf-8'))
                    
                    result_str = ''.join(content)
                    
                    # JSONパースを試みる
                    try:
                        result = json.loads(result_str)
                        if 'error' in result:
                            yield f"data: {json.dumps({'type': 'error', 'data': result['error']})}\n\n"
                        else:
                            yield f"data: {json.dumps(result)}\n\n"
                    except json.JSONDecodeError as e:
                        # JSONパースエラーの場合、生データを送信
                        yield f"data: {json.dumps({'type': 'raw', 'data': result_str})}\n\n"
                
                else:
                    yield f"data: {json.dumps({'type': 'error', 'data': f'Unknown content type: {content_type}'})}\n\n"
                    
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'data': str(e)})}\n\n"
        
        return Response(generate(), mimetype='text/event-stream')
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=80)
