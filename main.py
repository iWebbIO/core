import os
import re
import queue
import threading
import time
import json
import hashlib
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- CORRECTED: Missing imports added back ---
import requests
import m3u8
# -------------------------------------------

from flask import Flask, request, Response, jsonify, send_from_directory
from werkzeug.utils import secure_filename

# --- HTML Template ---
# The entire web interface is stored in this multi-line string.
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Webb Core</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f7f9; color: #333; margin: 0; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
        .tabs { display: flex; border-bottom: 2px solid #ddd; margin-bottom: 20px; }
        .tab-button { padding: 10px 20px; cursor: pointer; border: none; background: none; font-size: 18px; color: #888; border-bottom: 2px solid transparent; }
        .tab-button.active { color: #3498db; border-bottom-color: #3498db; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        h1, h2 { color: #2c3e50; text-align: center; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; vertical-align: middle; }
        th { background-color: #ecf0f1; }
        button, input[type="submit"] { padding: 8px 15px; background-color: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 14px; transition: background-color 0.3s; }
        button:hover, input[type="submit"]:hover { background-color: #2980b9; }
        .btn-danger { background-color: #e74c3c; }
        .btn-danger:hover { background-color: #c0392b; }
        .btn-secondary { background-color: #95a5a6; }
        .btn-secondary:hover { background-color: #7f8c8d; }
        .progress-bar-container { width: 100%; background-color: #e0e0e0; border-radius: 4px; overflow: hidden; }
        .progress-bar { height: 20px; background-color: #27ae60; width: 0%; text-align: center; color: white; line-height: 20px; font-size: 12px; transition: width 0.5s ease; }
        .status-completed { color: #27ae60; font-weight: bold; }
        .status-failed { color: #c0392b; font-weight: bold; }
        #duplicate-results { margin-top: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
        .duplicate-group { margin-bottom: 15px; }
        .duplicate-group p { font-weight: bold; }
        .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.8); }
        .modal-content { background-color: #111; margin: 5% auto; padding: 20px; border: 1px solid #888; width: 80%; max-width: 960px; }
        .close-modal { color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }
        video { width: 100%; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Webb Core</h1>
        <div class="tabs">
            <button class="tab-button active" onclick="showTab('downloader')">Downloader</button>
            <button class="tab-button" onclick="showTab('explorer')">File Explorer</button>
        </div>

        <!-- Downloader Tab -->
        <div id="downloader" class="tab-content active">
            <h2>M3U8 Downloader</h2>
            <form action="/add" method="POST" onsubmit="addDownload(event)">
                <input type="text" id="url-input" name="url" placeholder="Enter M3U8 URL here..." required style="flex-grow: 1; padding: 10px; font-size: 16px;">
                <label><input type="checkbox" name="auto_number" value="1"> Auto Number Files</label>
                <input type="submit" value="Add Download">
            </form>
            <table>
                <thead><tr><th>Filename</th><th style="width: 25%;">Progress</th><th>Size</th><th>Speed</th><th>Status</th></tr></thead>
                <tbody id="downloads-tbody"></tbody>
            </table>
        </div>

        <!-- File Explorer Tab -->
        <div id="explorer" class="tab-content">
            <h2>File Explorer</h2>
            <button onclick="refreshFileList()">Refresh Files</button>
            <button onclick="findDuplicates()" class="btn-secondary">Find Duplicate Files</button>
            <table>
                <thead><tr><th>Filename</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
                <tbody id="files-tbody"></tbody>
            </table>
            <div id="duplicate-results"></div>
        </div>
    </div>
    
    <!-- Video Player Modal -->
    <div id="videoModal" class="modal">
      <div class="modal-content">
        <span class="close-modal" onclick="closeModal()">&times;</span>
        <video id="videoPlayer" controls autoplay></video>
      </div>
    </div>

    <script>
        // --- Tab Management ---
        function showTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
            document.getElementById(tabName).classList.add('active');
            document.querySelector(`.tab-button[onclick="showTab('${tabName}')"]`).classList.add('active');
            if (tabName === 'explorer') refreshFileList();
        }

        // --- Downloader Logic ---
        function addDownload(event) {
            event.preventDefault();
            const urlInput = document.getElementById('url-input');
            const autoNumberCheckbox = document.querySelector('input[name="auto_number"]');
            const formData = new FormData();
            formData.append('url', urlInput.value);
            if (autoNumberCheckbox.checked) {
                formData.append('auto_number', '1');
            }
            fetch('/add', {
                method: 'POST',
                body: formData
            });
            urlInput.value = '';
        }

        function formatBytes(bytes, decimals = 2) {
            if (!+bytes) return '0 Bytes';
            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
        }

        const eventSource = new EventSource("/stream");
        eventSource.onmessage = function(event) {
            const jobs = JSON.parse(event.data);
            const tbody = document.getElementById('downloads-tbody');
            tbody.innerHTML = '';
            const sortedJobIds = Object.keys(jobs).sort((a, b) => b - a);
            for (const jobId of sortedJobIds) {
                const job = jobs[jobId];
                const speed = job.status === 'Downloading' ? `${formatBytes(job.speed)}/s` : 'N/A';
                const size = formatBytes(job.bytes_done);
                const progress = job.progress.toFixed(1);
                const statusClass = `status-${job.status.split(':')[0].toLowerCase().trim()}`;
                const row = `<tr>
                    <td>${job.filename}</td>
                    <td><div class="progress-bar-container"><div class="progress-bar" style="width: ${progress}%;">${progress}%</div></div></td>
                    <td>${size}</td><td>${speed}</td><td class="${statusClass}">${job.status}</td>
                </tr>`;
                tbody.innerHTML += row;
            }
        };

        // --- File Explorer Logic ---
        async function refreshFileList() {
            const response = await fetch('/files');
            const files = await response.json();
            const tbody = document.getElementById('files-tbody');
            tbody.innerHTML = '';
            files.forEach(file => {
                const row = `<tr>
                    <td>${file.name}</td>
                    <td>${formatBytes(file.size)}</td>
                    <td>${new Date(file.modified * 1000).toLocaleString()}</td>
                    <td>
                        <button onclick="watchFile('${file.name}')">Watch</button>
                        <button class="btn-secondary" onclick="renameFile('${file.name}')">Rename</button>
                        <button class="btn-danger" onclick="deleteFile('${file.name}')">Delete</button>
                    </td>
                </tr>`;
                tbody.innerHTML += row;
            });
        }
        
        async function renameFile(oldName) {
            const newName = prompt('Enter new filename:', oldName);
            if (newName && newName !== oldName) {
                await fetch('/rename', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ old_name: oldName, new_name: newName })
                });
                refreshFileList();
            }
        }

        async function deleteFile(fileName) {
            if (confirm(`Are you sure you want to delete ${fileName}?`)) {
                await fetch('/delete', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ filename: fileName })
                });
                refreshFileList();
            }
        }

        // --- Duplicate Finder Logic ---
        async function findDuplicates() {
            const resultsDiv = document.getElementById('duplicate-results');
            resultsDiv.innerHTML = '<p>Scanning for duplicates... This may take a while.</p>';
            const response = await fetch('/find_duplicates');
            const duplicates = await response.json();
            resultsDiv.innerHTML = '';
            if (Object.keys(duplicates).length === 0) {
                resultsDiv.innerHTML = '<p>No duplicate files found.</p>';
                return;
            }
            let content = '<h3>Duplicate Files Found</h3>';
            for (const hash in duplicates) {
                const files = duplicates[hash];
                content += `<div class="duplicate-group">
                    <p>Group (Size: ${formatBytes(files[0].size)}):</p>`;
                files.forEach((file, index) => {
                    content += `<div>
                        <input type="checkbox" id="dup-${hash}-${index}" value="${file.name}" ${index > 0 ? 'checked' : ''}>
                        <label for="dup-${hash}-${index}">${file.name}</label>
                    </div>`;
                });
                content += `</div>`;
            }
            content += `<button class="btn-danger" onclick="deleteSelectedDuplicates()">Delete Selected Duplicates</button>`;
            resultsDiv.innerHTML = content;
        }

        async function deleteSelectedDuplicates() {
            const checkboxes = document.querySelectorAll('#duplicate-results input[type="checkbox"]:checked');
            const filesToDelete = Array.from(checkboxes).map(cb => cb.value);
            if (filesToDelete.length === 0) {
                alert('No files selected for deletion.');
                return;
            }
            if (confirm(`Are you sure you want to delete ${filesToDelete.length} selected files?`)) {
                await fetch('/delete_duplicates', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ filenames: filesToDelete })
                });
                refreshFileList();
                document.getElementById('duplicate-results').innerHTML = '';
            }
        }

        // --- Video Modal Logic ---
        const modal = document.getElementById('videoModal');
        const videoPlayer = document.getElementById('videoPlayer');
        function watchFile(filename) {
            videoPlayer.src = `/video/${filename}`;
            modal.style.display = 'block';
        }
        function closeModal() {
            modal.style.display = 'none';
            videoPlayer.pause();
            videoPlayer.src = '';
        }
        window.onclick = function(event) {
            if (event.target == modal) closeModal();
        }
        
        // Initial load
        document.addEventListener('DOMContentLoaded', () => showTab('downloader'));
    </script>
</body>
</html>
"""

# --- Configuration & Global State ---
DOWNLOAD_DIR = "downloads"
MAX_WORKERS = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

app = Flask(__name__)
jobs = {}
job_counter = 0
lock = threading.Lock()
download_queue = queue.Queue()

# --- Helper Functions ---
def sanitize_filename(url):
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    if filename.endswith(".m3u8"):
        filename = filename[:-5] + ".mp4"
    return secure_filename(filename or parsed_url.netloc + ".mp4")

def get_unique_filepath(filepath):
    if not os.path.exists(filepath): return filepath
    directory, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)
    counter = 1
    while True:
        new_filepath = os.path.join(directory, f"{name} ({counter}){ext}")
        if not os.path.exists(new_filepath): return new_filepath
        counter += 1

def get_file_hashes():
    hashes = {}
    for filename in os.listdir(DOWNLOAD_DIR):
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.isfile(filepath) and os.path.getsize(filepath) > 0:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                buf = f.read(65536)
                while buf:
                    hasher.update(buf)
                    buf = f.read(65536)
            hashes[hasher.hexdigest()] = filename
    return hashes

# --- Downloader Backend ---
def downloader_worker():
    while True:
        job_id = download_queue.get()
        try:
            with lock: jobs[job_id]['status'] = 'Downloading'
            m3u8_obj = jobs[job_id]['m3u8_obj']

            segments = m3u8_obj.segments
            if not segments: raise ValueError("No media segments found.")
            with lock: jobs[job_id]['total_segments'] = len(segments)

            ts_data = [None] * len(segments)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(download_segment, seg.absolute_uri, job_id) for seg in segments]
                for i, future in enumerate(futures):
                    ts_data[i] = future.result()

            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            initial_path = os.path.join(DOWNLOAD_DIR, jobs[job_id]['filename'])
            unique_path = get_unique_filepath(initial_path)
            with lock: jobs[job_id]['filename'] = os.path.basename(unique_path)

            with open(unique_path, 'wb') as f:
                for data in ts_data:
                    if data: f.write(data)
            
            with lock:
                jobs[job_id]['status'] = 'Completed'
                jobs[job_id]['bytes_done'] = os.path.getsize(unique_path)
                jobs[job_id]['progress'] = 100.0
        except Exception as e:
            with lock: jobs[job_id]['status'] = f"Failed: {str(e)[:50]}..."

def download_segment(url, job_id):
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=10)
        response.raise_for_status()
        segment_data = response.content
        with lock:
            job = jobs[job_id]
            job['segments_done'] += 1
            job['bytes_done'] += len(segment_data)
            job['progress'] = (job['segments_done'] / job['total_segments']) * 100
            elapsed = time.time() - job['start_time']
            if elapsed > 0: job['speed'] = job['bytes_done'] / elapsed
        return segment_data
    except requests.RequestException: return None

# --- Flask Routes ---
@app.route('/')
def index():
    return HTML_TEMPLATE

@app.route('/add', methods=['POST'])
def add_download():
    global job_counter
    url = request.form.get('url')
    auto_number = request.form.get('auto_number') == '1'
    if not url or ".m3u8" not in url: return jsonify({"error": "Invalid URL"}), 400

    headers = {'User-Agent': USER_AGENT}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        m3u8_obj = m3u8.loads(response.text, uri=url)
        if m3u8_obj.is_variant:
            best_stream = sorted(m3u8_obj.playlists, key=lambda p: p.stream_info.bandwidth, reverse=True)[0]
            playlist_url = best_stream.absolute_uri
            response = requests.get(playlist_url, headers=headers, timeout=15)
            response.raise_for_status()
            m3u8_content = response.text
            m3u8_obj = m3u8.loads(m3u8_content, uri=playlist_url)
        else:
            m3u8_content = response.text
        m3u8_hash = hashlib.md5(m3u8_content.encode()).hexdigest()
    except Exception as e:
        return jsonify({"error": f"Failed to fetch M3U8: {str(e)}"}), 400

    hashes = get_file_hashes()
    if m3u8_hash in hashes:
        return jsonify({"error": "Duplicate file already exists"}), 400

    if auto_number:
        files = os.listdir(DOWNLOAD_DIR)
        nums = [int(f[:-4]) for f in files if f.endswith('.mp4') and f[:-4].isdigit()]
        # Also include numbers from current jobs
        with lock:
            job_nums = [int(job['filename'][:-4]) for job in jobs.values() if job['filename'].endswith('.mp4') and job['filename'][:-4].isdigit()]
        all_nums = nums + job_nums
        next_num = max(all_nums) + 1 if all_nums else 1
        filename = f"{next_num}.mp4"
    else:
        filename = sanitize_filename(url)

    with lock:
        job_counter += 1
        job_id = job_counter
        jobs[job_id] = {'url': url, 'filename': filename, 'status': 'Pending', 'progress': 0, 'segments_done': 0, 'total_segments': 1, 'bytes_done': 0, 'speed': 0, 'start_time': time.time(), 'm3u8_obj': m3u8_obj, 'm3u8_hash': m3u8_hash}
    download_queue.put(job_id)
    return jsonify({"message": "Download added"}), 200

@app.route('/stream')
def stream():
    def event_stream():
        while True:
            with lock:
                # Create a copy of jobs without the m3u8_obj for JSON serialization
                jobs_copy = {}
                for job_id, job in jobs.items():
                    jobs_copy[job_id] = {k: v for k, v in job.items() if k != 'm3u8_obj'}
                data = json.dumps(jobs_copy)
            yield f"data: {data}\n\n"
            time.sleep(1)
    return Response(event_stream(), mimetype='text/event-stream')

# --- File Explorer API Routes ---
@app.route('/files')
def list_files():
    files = []
    for filename in os.listdir(DOWNLOAD_DIR):
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.isfile(filepath):
            files.append({
                'name': filename,
                'size': os.path.getsize(filepath),
                'modified': os.path.getmtime(filepath)
            })
    return jsonify(sorted(files, key=lambda x: x['modified'], reverse=True))

@app.route('/rename', methods=['POST'])
def rename_file():
    data = request.json
    old_name = secure_filename(data['old_name'])
    new_name = secure_filename(data['new_name'])
    old_path = os.path.join(DOWNLOAD_DIR, old_name)
    new_path = os.path.join(DOWNLOAD_DIR, new_name)
    if os.path.exists(old_path) and not os.path.exists(new_path):
        os.rename(old_path, new_path)
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'File operation failed'}), 400

@app.route('/delete', methods=['POST'])
def delete_file():
    filename = secure_filename(request.json['filename'])
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(filepath):
        os.remove(filepath)
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'File not found'}), 404

@app.route('/video/<filename>')
def video_stream(filename):
    return send_from_directory(DOWNLOAD_DIR, secure_filename(filename))

# --- Duplicate Finder API Routes ---
@app.route('/find_duplicates')
def find_duplicates():
    files_by_size = {}
    for filename in os.listdir(DOWNLOAD_DIR):
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.isfile(filepath):
            size = os.path.getsize(filepath)
            if size > 0:
                files_by_size.setdefault(size, []).append({'name': filename, 'path': filepath, 'size': size})

    hashes = {}
    for size, files in files_by_size.items():
        if len(files) > 1:
            for file_info in files:
                hasher = hashlib.md5()
                with open(file_info['path'], 'rb') as f:
                    buf = f.read(65536)
                    while len(buf) > 0:
                        hasher.update(buf)
                        buf = f.read(65536)
                file_hash = hasher.hexdigest()
                hashes.setdefault(file_hash, []).append(file_info)
    
    duplicates = {h: f for h, f in hashes.items() if len(f) > 1}
    return jsonify(duplicates)

@app.route('/delete_duplicates', methods=['POST'])
def delete_duplicates():
    filenames = request.json['filenames']
    for filename in filenames:
        filepath = os.path.join(DOWNLOAD_DIR, secure_filename(filename))
        if os.path.exists(filepath):
            os.remove(filepath)
    return jsonify({'success': True})

# --- Main Execution ---
if __name__ == '__main__':
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    worker = threading.Thread(target=downloader_worker, daemon=True)
    worker.start()
    print("🚀 Webb Core is running!")
    print(f"🌍 Access it at: http://localhost:8080")
    app.run(host='0.0.0.0', port=8080, threaded=True)
