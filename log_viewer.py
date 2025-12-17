from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LOG_FILE = "/root/scraper/master_controller.log"

@app.get("/")
async def root():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>スクレイピング監視ダッシュボード</title>
        <style>
            body {
                background: #0a0e27;
                color: #00ff41;
                font-family: 'Courier New', monospace;
                margin: 0;
                padding: 20px;
            }
            .header {
                border-bottom: 2px solid #00ff41;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }
            h1 {
                margin: 0;
                font-size: 24px;
                text-shadow: 0 0 10px #00ff41;
            }
            .stats {
                display: flex;
                gap: 20px;
                margin-bottom: 20px;
            }
            .stat-box {
                background: #1a1f3a;
                border: 1px solid #00ff41;
                padding: 10px 20px;
                border-radius: 5px;
            }
            #log-container {
                background: #000;
                border: 2px solid #00ff41;
                padding: 15px;
                height: 600px;
                overflow-y: auto;
                font-size: 13px;
                line-height: 1.4;
            }
            .log-line {
                margin: 2px 0;
                padding: 2px 5px;
            }
            .new-product {
                background: #ff4444;
                color: #fff;
                font-weight: bold;
                padding: 5px;
                animation: blink 1s infinite;
            }
            @keyframes blink {
                0%, 50% { opacity: 1; }
                25%, 75% { opacity: 0.7; }
            }
            .success {
                color: #00ff41;
            }
            .info {
                color: #4a9eff;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🔍 リアルタイムスクレイピング監視システム</h1>
        </div>
        <div class="stats">
            <div class="stat-box">
                <div>📊 監視サイト数</div>
                <div style="font-size: 28px; font-weight: bold;">43</div>
            </div>
            <div class="stat-box">
                <div>⏱️ 稼働時間</div>
                <div style="font-size: 28px; font-weight: bold;">24/7</div>
            </div>
            <div class="stat-box">
                <div>🎉 新商品検知</div>
                <div style="font-size: 28px; font-weight: bold; color: #ff4444;" id="new-count">0</div>
            </div>
        </div>
        <div id="log-container"></div>
        
        <script>
            const logContainer = document.getElementById('log-container');
            const newCountEl = document.getElementById('new-count');
            let newProductCount = 0;
            
            const eventSource = new EventSource('/logs/stream');
            
            eventSource.onmessage = function(event) {
                const line = event.data;
                const logDiv = document.createElement('div');
                logDiv.className = 'log-line';
                
                if (line.includes('🎉 新商品検知') || line.includes('新1位:')) {
                    logDiv.className = 'log-line new-product';
                    newProductCount++;
                    newCountEl.textContent = newProductCount;
                } else if (line.includes('✅')) {
                    logDiv.className = 'log-line success';
                } else if (line.includes('[INFO]')) {
                    logDiv.className = 'log-line info';
                }
                
                logDiv.textContent = line;
                logContainer.appendChild(logDiv);
                logContainer.scrollTop = logContainer.scrollHeight;
                
                if (logContainer.children.length > 500) {
                    logContainer.removeChild(logContainer.firstChild);
                }
            };
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

@app.get("/logs/stream")
async def stream_logs():
    async def generate():
        if not os.path.exists(LOG_FILE):
            yield f"data: ⚠️ ログファイルが見つかりません: {LOG_FILE}\n\n"
            return
            
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines[-100:]:
                yield f"data: {line.strip()}\n\n"
                await asyncio.sleep(0.05)
            
            while True:
                line = f.readline()
                if line:
                    yield f"data: {line.strip()}\n\n"
                else:
                    await asyncio.sleep(1)
    
    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/health")
async def health():
    return {"status": "ok", "log_file": LOG_FILE}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
