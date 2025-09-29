import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import concurrent.futures
import queue
import threading
import os
import json
import time
import logging
import requests
import datetime
import uuid
import subprocess
import sys

# --- Dependency Check ---
try:
    import yt_dlp
    YT_DLP_AVAILABLE = True
except ImportError:
    YT_DLP_AVAILABLE = False

# --- Constants ---
APP_VERSION = "2.1" # Version bump
SETTINGS_FILE = "m3udl_settings.json"
LOG_FILE = "m3udl_app.log"

# --- Set up Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# --- Default Settings ---
### MODIFIED ### - Added a detailed yt_dlp_options dictionary
DEFAULT_SETTINGS = {
    "simultaneous_downloads": 3,
    "max_retries": 3,
    "retry_delay": 5, # seconds
    "output_format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best", # Kept for display/fallback
    "proxy": "",
    "user_agent": "",
    "speed_limit": "",
    "autopilot": True,
    "use_yt_dlp": True,
    "theme": "System",
    "enable_scheduling": False,
    "start_time": "00:00",
    "end_time": "23:59",
    "yt_dlp_options": { # NEW detailed options structure
        "video_quality": "best", # 'best', '1080p', '720p', '480p'
        "audio_quality": "best", # 'best', 'worst'
        "audio_format": "m4a",   # 'm4a', 'mp3', 'opus', 'wav'
        "output_template": "%(title)s.%(ext)s",
        "download_subs": False,
        "sub_lang": "en",
        "embed_subs": False,
        "embed_thumbnail": False,
        "embed_metadata": False,
        "convert_video": "none" # 'none', 'mp4', 'mkv', 'webm'
    }
}

# --- Download Statuses ---
STATUS_QUEUED = "Queued"
STATUS_DOWNLOADING = "Downloading"
STATUS_COMPLETED = "Completed"
STATUS_ERROR = "Error"
STATUS_CANCELLED = "Cancelled"


### NEW ### - The entire class for the advanced configuration window
class YTDLPConfigWindow(ctk.CTkToplevel):
    def __init__(self, master, current_options):
        super().__init__(master)
        self.master = master
        self.transient(master) # Keep window on top of the main app
        self.grab_set() # Modal - block interaction with the main window
        self.title("Advanced yt-dlp Options")
        self.geometry("600x650")

        self.options = current_options.copy() # Work with a copy

        # --- Create variables for widgets ---
        self.video_quality_var = ctk.StringVar(value=self.options.get("video_quality", "best"))
        self.audio_quality_var = ctk.StringVar(value=self.options.get("audio_quality", "best"))
        self.audio_format_var = ctk.StringVar(value=self.options.get("audio_format", "m4a"))
        self.output_template_var = ctk.StringVar(value=self.options.get("output_template", "%(title)s.%(ext)s"))
        self.download_subs_var = ctk.BooleanVar(value=self.options.get("download_subs", False))
        self.sub_lang_var = ctk.StringVar(value=self.options.get("sub_lang", "en"))
        self.embed_subs_var = ctk.BooleanVar(value=self.options.get("embed_subs", False))
        self.embed_thumbnail_var = ctk.BooleanVar(value=self.options.get("embed_thumbnail", False))
        self.embed_metadata_var = ctk.BooleanVar(value=self.options.get("embed_metadata", False))
        self.convert_video_var = ctk.StringVar(value=self.options.get("convert_video", "none"))

        self.create_widgets()

    def create_widgets(self):
        main_frame = ctk.CTkFrame(self)
        main_frame.pack(padx=15, pady=15, fill="both", expand=True)

        # --- Format Selection Frame ---
        format_frame = ctk.CTkFrame(main_frame)
        format_frame.pack(fill="x", padx=10, pady=10)
        format_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(format_frame, text="Format Selection", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5, sticky="w")

        ctk.CTkLabel(format_frame, text="Video Quality:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkOptionMenu(format_frame, variable=self.video_quality_var, values=["best", "1080p", "720p", "480p", "worst"]).grid(row=1, column=1, padx=10, pady=5, sticky="w")

        ctk.CTkLabel(format_frame, text="Audio Quality:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkOptionMenu(format_frame, variable=self.audio_quality_var, values=["best", "worst"]).grid(row=2, column=1, padx=10, pady=5, sticky="w")

        # --- Output Frame ---
        output_frame = ctk.CTkFrame(main_frame)
        output_frame.pack(fill="x", padx=10, pady=10)
        output_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(output_frame, text="Output", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5, sticky="w")

        ctk.CTkLabel(output_frame, text="Filename Template:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkEntry(output_frame, textvariable=self.output_template_var).grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        # --- Subtitles Frame ---
        subs_frame = ctk.CTkFrame(main_frame)
        subs_frame.pack(fill="x", padx=10, pady=10)
        subs_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(subs_frame, text="Subtitles", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5, sticky="w")

        ctk.CTkCheckBox(subs_frame, text="Download Subtitles", variable=self.download_subs_var).grid(row=1, column=0, columnspan=2, padx=10, pady=5, sticky="w")
        ctk.CTkLabel(subs_frame, text="Language (e.g., en, es):").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkEntry(subs_frame, textvariable=self.sub_lang_var, width=80).grid(row=2, column=1, padx=10, pady=5, sticky="w")

        # --- Post-Processing Frame ---
        post_frame = ctk.CTkFrame(main_frame)
        post_frame.pack(fill="x", padx=10, pady=10)
        post_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(post_frame, text="Post-Processing", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5, sticky="w")

        ctk.CTkCheckBox(post_frame, text="Embed Subtitles into video file", variable=self.embed_subs_var).grid(row=1, column=0, columnspan=2, padx=10, pady=5, sticky="w")
        ctk.CTkCheckBox(post_frame, text="Embed Thumbnail into video file", variable=self.embed_thumbnail_var).grid(row=2, column=0, columnspan=2, padx=10, pady=5, sticky="w")
        ctk.CTkCheckBox(post_frame, text="Embed Metadata into video file", variable=self.embed_metadata_var).grid(row=3, column=0, columnspan=2, padx=10, pady=5, sticky="w")

        ctk.CTkLabel(post_frame, text="Convert Audio Format:").grid(row=4, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkOptionMenu(post_frame, variable=self.audio_format_var, values=["m4a", "mp3", "opus", "wav", "flac"]).grid(row=4, column=1, padx=10, pady=5, sticky="w")
        
        ctk.CTkLabel(post_frame, text="Convert Video Container:").grid(row=5, column=0, padx=10, pady=5, sticky="w")
        ctk.CTkOptionMenu(post_frame, variable=self.convert_video_var, values=["none", "mp4", "mkv", "webm"]).grid(row=5, column=1, padx=10, pady=5, sticky="w")

        # --- Action Buttons ---
        button_frame = ctk.CTkFrame(self, fg_color="transparent")
        button_frame.pack(fill="x", padx=15, pady=(0, 15))
        ctk.CTkButton(button_frame, text="Cancel", command=self.destroy).pack(side="right", padx=5)
        ctk.CTkButton(button_frame, text="Save and Apply", command=self.apply_and_close).pack(side="right", padx=5)

    def apply_and_close(self):
        """Collects data from widgets and sends it back to the main app."""
        new_options = {
            "video_quality": self.video_quality_var.get(),
            "audio_quality": self.audio_quality_var.get(),
            "audio_format": self.audio_format_var.get(),
            "output_template": self.output_template_var.get(),
            "download_subs": self.download_subs_var.get(),
            "sub_lang": self.sub_lang_var.get(),
            "embed_subs": self.embed_subs_var.get(),
            "embed_thumbnail": self.embed_thumbnail_var.get(),
            "embed_metadata": self.embed_metadata_var.get(),
            "convert_video": self.convert_video_var.get()
        }
        self.master.update_yt_dlp_settings(new_options)
        self.destroy()


class M3UDLApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(f"M3UDL - Ultimate Video Downloader v{APP_VERSION}")
        self.geometry("1100x750")

        self.settings = self.load_settings()
        ctk.set_appearance_mode(self.settings["theme"])
        ctk.set_default_color_theme("blue")

        self.tasks = {}  # Central dictionary to track all download tasks
        self.download_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.settings["simultaneous_downloads"])

        self.create_widgets()
        self.start_autopilot()

        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self):
        # Main container
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.tabview = ctk.CTkTabview(self, width=1050, height=680)
        self.tabview.pack(pady=10, padx=10, fill="both", expand=True)

        self.tabview.add("Add Downloads")
        self.tabview.add("Download Manager")
        self.tabview.add("Settings")
        self.tabview.add("Scheduling")
        self.tabview.add("Log")

        self.create_add_downloads_tab()
        self.create_download_manager_tab()
        self.create_settings_tab()
        self.create_scheduling_tab()
        self.create_log_tab()

    # --- Tab Creation Methods ---
    def create_add_downloads_tab(self):
        tab = self.tabview.tab("Add Downloads")
        tab.grid_columnconfigure(0, weight=1)

        # Single Download Frame
        single_frame = ctk.CTkFrame(tab, corner_radius=10)
        single_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        single_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(single_frame, text="Single URL", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 5))
        ctk.CTkLabel(single_frame, text="URL:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.url_entry = ctk.CTkEntry(single_frame, placeholder_text="Enter video or M3U8 URL")
        self.url_entry.grid(row=1, column=1, columnspan=2, padx=10, pady=5, sticky="ew")

        ctk.CTkLabel(single_frame, text="Output Folder:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.output_entry = ctk.CTkEntry(single_frame)
        self.output_entry.grid(row=2, column=1, padx=(10, 0), pady=5, sticky="ew")
        self.output_entry.insert(0, os.getcwd())
        ctk.CTkButton(single_frame, text="Browse", width=80, command=self.browse_output).grid(row=2, column=2, padx=(5, 10), pady=5)

        self.add_to_queue_btn = ctk.CTkButton(single_frame, text="Add to Queue", command=self.add_single_to_queue)
        self.add_to_queue_btn.grid(row=3, column=1, columnspan=2, padx=10, pady=(5, 10))

        # Bulk Download Frame
        bulk_frame = ctk.CTkFrame(tab, corner_radius=10)
        bulk_frame.grid(row=1, column=0, padx=10, pady=10, sticky="ew")
        bulk_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(bulk_frame, text="Bulk from File", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 5))
        ctk.CTkLabel(bulk_frame, text="Text File:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.bulk_file_entry = ctk.CTkEntry(bulk_frame, placeholder_text="Select a .txt file with one URL per line")
        self.bulk_file_entry.grid(row=1, column=1, padx=(10, 0), pady=5, sticky="ew")
        ctk.CTkButton(bulk_frame, text="Browse", width=80, command=self.browse_bulk_file).grid(row=1, column=2, padx=(5, 10), pady=5)

        ctk.CTkLabel(bulk_frame, text="Output Folder:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.bulk_output_entry = ctk.CTkEntry(bulk_frame)
        self.bulk_output_entry.grid(row=2, column=1, padx=(10, 0), pady=5, sticky="ew")
        self.bulk_output_entry.insert(0, os.getcwd())
        ctk.CTkButton(bulk_frame, text="Browse", width=80, command=self.browse_bulk_output).grid(row=2, column=2, padx=(5, 10), pady=5)

        self.bulk_download_btn = ctk.CTkButton(bulk_frame, text="Add Bulk to Queue", command=self.add_bulk_to_queue)
        self.bulk_download_btn.grid(row=3, column=1, columnspan=2, padx=10, pady=(5, 10))

    def create_download_manager_tab(self):
        tab = self.tabview.tab("Download Manager")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(1, weight=1)

        btn_frame = ctk.CTkFrame(tab)
        btn_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        self.start_queue_btn = ctk.CTkButton(btn_frame, text="Start Queue", command=self.start_queue)
        self.start_queue_btn.pack(side="left", padx=5)
        self.stop_queue_btn = ctk.CTkButton(btn_frame, text="Stop All", command=self.stop_queue, fg_color="red", hover_color="darkred")
        self.stop_queue_btn.pack(side="left", padx=5)
        self.clear_completed_btn = ctk.CTkButton(btn_frame, text="Clear Completed", command=self.clear_completed)
        self.clear_completed_btn.pack(side="right", padx=5)

        self.scrollable_frame = ctk.CTkScrollableFrame(tab, label_text="Downloads")
        self.scrollable_frame.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="nsew")
        self.scrollable_frame.grid_columnconfigure(0, weight=1)

    def create_settings_tab(self):
        settings_frame = self.tabview.tab("Settings")
        settings_frame.grid_columnconfigure(0, weight=1)
        
        # Appearance Settings
        appearance_frame = ctk.CTkFrame(settings_frame)
        appearance_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        appearance_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(appearance_frame, text="Appearance", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5)
        ctk.CTkLabel(appearance_frame, text="Theme:").grid(row=1, column=0, sticky="w", padx=10, pady=5)
        self.theme_menu = ctk.CTkOptionMenu(appearance_frame, values=["System", "Dark", "Light"], command=self.change_theme)
        self.theme_menu.set(self.settings["theme"].capitalize())
        self.theme_menu.grid(row=1, column=1, sticky="w", padx=10, pady=5)

        # Download Settings
        download_frame = ctk.CTkFrame(settings_frame)
        download_frame.grid(row=1, column=0, padx=10, pady=10, sticky="ew")
        download_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(download_frame, text="Download", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5)
        ctk.CTkLabel(download_frame, text="Simultaneous Downloads:").grid(row=1, column=0, sticky="w", padx=10, pady=5)
        self.sim_downloads_label = ctk.CTkLabel(download_frame, text=str(self.settings["simultaneous_downloads"]))
        self.sim_downloads_label.grid(row=1, column=2, padx=10)
        self.sim_downloads_slider = ctk.CTkSlider(download_frame, from_=1, to=10, number_of_steps=9, command=self.update_sim_downloads_display)
        self.sim_downloads_slider.grid(row=1, column=1, sticky="ew", padx=10, pady=5)
        self.sim_downloads_slider.set(self.settings["simultaneous_downloads"])
        
        ctk.CTkLabel(download_frame, text="Max Retries:").grid(row=2, column=0, sticky="w", padx=10, pady=5)
        self.max_retries_entry = ctk.CTkEntry(download_frame)
        self.max_retries_entry.grid(row=2, column=1, columnspan=2, sticky="ew", padx=10, pady=5)
        self.max_retries_entry.insert(0, str(self.settings["max_retries"]))
        
        # Output Settings
        output_settings_frame = ctk.CTkFrame(settings_frame)
        output_settings_frame.grid(row=2, column=0, padx=10, pady=10, sticky="ew")
        output_settings_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(output_settings_frame, text="Output", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5)
        
        ### MODIFIED ### - Changed label and added the new button
        ctk.CTkLabel(output_settings_frame, text="yt-dlp Format (Summary):").grid(row=1, column=0, sticky="w", padx=10, pady=5)
        self.format_entry = ctk.CTkEntry(output_settings_frame, state="readonly") # Readonly, as it's now a summary
        self.format_entry.grid(row=1, column=1, sticky="ew", padx=10, pady=5)
        self.update_format_summary() # Populate it on startup

        ### NEW ### - The button to open the advanced config window
        ctk.CTkButton(
            output_settings_frame,
            text="Advanced yt-dlp Options...",
            command=self.open_yt_dlp_config_window
        ).grid(row=2, column=1, sticky="e", padx=10, pady=10)


        # Network Settings
        network_frame = ctk.CTkFrame(settings_frame)
        network_frame.grid(row=3, column=0, padx=10, pady=10, sticky="ew")
        network_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(network_frame, text="Network", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=5)
        ctk.CTkLabel(network_frame, text="Speed Limit (e.g., 1M):").grid(row=1, column=0, sticky="w", padx=10, pady=5)
        self.speed_entry = ctk.CTkEntry(network_frame)
        self.speed_entry.grid(row=1, column=1, sticky="ew", padx=10, pady=5)
        self.speed_entry.insert(0, self.settings["speed_limit"])
        ctk.CTkLabel(network_frame, text="Proxy:").grid(row=2, column=0, sticky="w", padx=10, pady=5)
        self.proxy_entry = ctk.CTkEntry(network_frame, placeholder_text="http://user:pass@host:port")
        self.proxy_entry.grid(row=2, column=1, sticky="ew", padx=10, pady=5)
        self.proxy_entry.insert(0, self.settings["proxy"])
        ctk.CTkLabel(network_frame, text="User Agent:").grid(row=3, column=0, sticky="w", padx=10, pady=5)
        self.ua_entry = ctk.CTkEntry(network_frame)
        self.ua_entry.grid(row=3, column=1, sticky="ew", padx=10, pady=5)
        self.ua_entry.insert(0, self.settings["user_agent"])

        # Advanced Settings
        advanced_frame = ctk.CTkFrame(settings_frame)
        advanced_frame.grid(row=4, column=0, padx=10, pady=10, sticky="ew")
        ctk.CTkLabel(advanced_frame, text="Advanced", font=ctk.CTkFont(weight="bold")).pack(pady=5)
        self.autopilot_var = ctk.BooleanVar(value=self.settings["autopilot"])
        ctk.CTkCheckBox(advanced_frame, text="Autopilot Mode (start downloads as they are added)", variable=self.autopilot_var).pack(pady=5, padx=10, anchor="w")
        self.yt_dlp_var = ctk.BooleanVar(value=self.settings["use_yt_dlp"])
        yt_dlp_checkbox = ctk.CTkCheckBox(advanced_frame, text="Use yt-dlp (required for M3U8/HLS)", variable=self.yt_dlp_var)
        if not YT_DLP_AVAILABLE:
            yt_dlp_checkbox.configure(state="disabled", text="Use yt-dlp (Not installed!)")
            self.yt_dlp_var.set(False)
        yt_dlp_checkbox.pack(pady=5, padx=10, anchor="w")
        
        ctk.CTkButton(settings_frame, text="Save Settings", command=self.save_settings).grid(row=6, column=0, pady=20)

    def create_scheduling_tab(self):
        """Creates the UI for the dedicated Scheduling tab."""
        tab = self.tabview.tab("Scheduling")
        tab.grid_columnconfigure(0, weight=1)

        # Main container frame for padding
        main_frame = ctk.CTkFrame(tab)
        main_frame.grid(row=0, column=0, padx=20, pady=20, sticky="ew")
        main_frame.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(main_frame, text="Download Scheduling", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=3, pady=(5, 15))
        
        self.schedule_var = ctk.BooleanVar(value=self.settings["enable_scheduling"])
        ctk.CTkCheckBox(main_frame, text="Enable Download Schedule", variable=self.schedule_var, font=ctk.CTkFont(size=14)).grid(row=1, column=0, columnspan=3, padx=10, pady=(5,15), sticky="w")

        ctk.CTkLabel(main_frame, text="Start Time (HH:MM):").grid(row=2, column=0, padx=10, pady=10, sticky="w")
        self.start_time_entry = ctk.CTkEntry(main_frame, width=120)
        self.start_time_entry.grid(row=2, column=1, padx=10, pady=10, sticky="w")
        self.start_time_entry.insert(0, self.settings["start_time"])

        ctk.CTkLabel(main_frame, text="End Time (HH:MM):").grid(row=3, column=0, padx=10, pady=10, sticky="w")
        self.end_time_entry = ctk.CTkEntry(main_frame, width=120)
        self.end_time_entry.grid(row=3, column=1, padx=10, pady=10, sticky="w")
        self.end_time_entry.insert(0, self.settings["end_time"])
        
        info_label = ctk.CTkLabel(main_frame, text="When scheduling is enabled, new downloads will only begin between the specified start and end times.\nThis feature is checked by Autopilot and when you manually click 'Start Queue'.", wraplength=400, justify="left")
        info_label.grid(row=4, column=0, columnspan=3, padx=10, pady=20, sticky="w")

        ctk.CTkButton(tab, text="Save Settings", command=self.save_settings).grid(row=1, column=0, pady=20)


    def create_log_tab(self):
        tab = self.tabview.tab("Log")
        self.log_text = ctk.CTkTextbox(tab, state="disabled")
        self.log_text.pack(fill="both", expand=True, padx=10, pady=10)

    # --- UI Update and Task Management ---

    def add_task(self, url, output_path):
        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "url": url,
            "output_path": output_path,
            "status": STATUS_QUEUED,
            "progress": 0,
            "retries": 0,
            "future": None,
            "ui": {},
            "final_filepath": None
        }
        self.tasks[task_id] = task
        self.create_task_ui(task_id)
        self.download_queue.put(task_id)
        self.log(f"Queued: {url}")
        return task_id

    def create_task_ui(self, task_id):
        task = self.tasks[task_id]
        frame = ctk.CTkFrame(self.scrollable_frame)
        frame.grid(sticky="ew", padx=5, pady=5)
        frame.grid_columnconfigure(0, weight=1)
        task["ui"]["frame"] = frame

        # Filename/URL Label
        filename_label = ctk.CTkLabel(frame, text=os.path.basename(task['url']) or task['url'], anchor="w")
        filename_label.grid(row=0, column=0, padx=10, pady=2, sticky="ew")
        task["ui"]["filename"] = filename_label

        # Progress Bar
        progress_bar = ctk.CTkProgressBar(frame)
        progress_bar.set(0)
        progress_bar.grid(row=1, column=0, padx=10, pady=2, sticky="ew")
        task["ui"]["progress"] = progress_bar

        # Status Label
        status_label = ctk.CTkLabel(frame, text=STATUS_QUEUED, anchor="w")
        status_label.grid(row=1, column=1, padx=10, pady=2, sticky="w")
        task["ui"]["status"] = status_label

        # Action Buttons Frame
        action_frame = ctk.CTkFrame(frame, fg_color="transparent")
        action_frame.grid(row=0, column=1, rowspan=2, padx=10, pady=2, sticky="e")
        task["ui"]["action_frame"] = action_frame

        cancel_btn = ctk.CTkButton(action_frame, text="Cancel", width=70, command=lambda t=task_id: self.cancel_task(t))
        cancel_btn.pack()
        task["ui"]["cancel_btn"] = cancel_btn

    def update_ui_safe(self, task_id, **kwargs):
        """Safely schedules a UI update on the main thread."""
        self.after(0, self._update_ui, task_id, kwargs)

    def _update_ui(self, task_id, updates):
        if task_id not in self.tasks:
            return
        task = self.tasks[task_id]
        ui = task["ui"]

        if "status" in updates:
            status = updates["status"]
            task["status"] = status
            ui["status"].configure(text=status)
            if status == STATUS_COMPLETED:
                ui["status"].configure(text_color="green")
                ui["progress"].set(1)
                ui["cancel_btn"].pack_forget()
                open_btn = ctk.CTkButton(ui["action_frame"], text="Open Folder", width=100,
                                         command=lambda p=task["final_filepath"]: self.open_file_location(p))
                open_btn.pack()
            elif status == STATUS_ERROR:
                ui["status"].configure(text_color="red")
            elif status == STATUS_CANCELLED:
                ui["status"].configure(text_color="gray")
                ui["cancel_btn"].pack_forget()

        if "progress" in updates:
            progress = updates["progress"]
            task["progress"] = progress
            ui["progress"].set(progress)

        if "filename" in updates:
            ui["filename"].configure(text=updates["filename"])

    # --- Core Logic ---

    def process_queue(self):
        """Processes items from the queue if conditions are met."""
        if self.stop_event.is_set():
            return
            
        if self.settings["enable_scheduling"]:
            if not self.is_within_schedule():
                # Log this state occasionally so the user knows why nothing is happening
                if not hasattr(self, 'last_schedule_log_time') or time.time() - self.last_schedule_log_time > 300: # Log every 5 mins
                    self.log("Queue processing paused due to schedule.")
                    self.last_schedule_log_time = time.time()
                return

        active_downloads = sum(1 for t in self.tasks.values() if t["status"] == STATUS_DOWNLOADING)
        
        while not self.download_queue.empty() and active_downloads < self.settings["simultaneous_downloads"]:
            try:
                task_id = self.download_queue.get_nowait()
                if self.tasks[task_id]["status"] == STATUS_QUEUED:
                    self.update_ui_safe(task_id, status=STATUS_DOWNLOADING)
                    future = self.executor.submit(self.download_video, task_id)
                    self.tasks[task_id]["future"] = future
                    future.add_done_callback(self.on_download_done)
                    active_downloads += 1
            except queue.Empty:
                break # Queue is empty, exit loop

    ### MODIFIED ### - This is the core logic change. It now builds ydl_opts from the detailed settings.
    def download_video(self, task_id):
        """Worker function to download a video. Runs in a separate thread."""
        task = self.tasks[task_id]
        url = task["url"]
        output_path = task["output_path"]
        opts = self.settings.get("yt_dlp_options", {}) # Get our detailed options

        def progress_hook(d):
            if self.tasks[task_id]['status'] == STATUS_CANCELLED:
                raise yt_dlp.utils.DownloadError("Download cancelled by user.")

            if d['status'] == 'downloading':
                total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
                if total_bytes:
                    progress = d['downloaded_bytes'] / total_bytes
                    self.update_ui_safe(task_id, progress=progress)
            elif d['status'] == 'finished':
                task['final_filepath'] = d.get('filename')
                self.update_ui_safe(task_id, progress=1.0)

        if self.settings["use_yt_dlp"] and YT_DLP_AVAILABLE:
            try:
                # --- Build format string from options ---
                video_quality_map = {
                    "best": "bestvideo", "1080p": "bestvideo[height<=1080]",
                    "720p": "bestvideo[height<=720]", "480p": "bestvideo[height<=480]", "worst": "worstvideo"
                }
                audio_quality_map = {"best": "bestaudio", "worst": "worstaudio"}
                
                video_part = video_quality_map.get(opts.get("video_quality", "best"), "bestvideo")
                audio_part = audio_quality_map.get(opts.get("audio_quality", "best"), "bestaudio")
                
                format_string = f"{video_part}[ext=mp4]+{audio_part}[ext=m4a]/{video_part}+{audio_part}/best"

                # --- Build postprocessors list ---
                postprocessors = []
                if opts.get("embed_thumbnail"):
                    postprocessors.append({'key': 'EmbedThumbnail', 'already_have_thumbnail': False})
                if opts.get("embed_metadata"):
                    postprocessors.append({'key': 'FFmpegMetadata', 'add_metadata': True})
                if opts.get("embed_subs") and opts.get("download_subs"):
                    postprocessors.append({'key': 'FFmpegEmbedSubtitle'})
                if opts.get("audio_format") not in ["m4a"]: # m4a is often the default, no conversion needed
                    postprocessors.append({'key': 'FFmpegExtractAudio', 'preferredcodec': opts.get("audio_format", "m4a")})
                if opts.get("convert_video") != "none":
                     postprocessors.append({'key': 'FFmpegVideoConvertor', 'preferedformat': opts.get("convert_video")})

                # --- Build the final ydl_opts dictionary ---
                ydl_opts = {
                    'outtmpl': os.path.join(output_path, opts.get("output_template", "%(title)s.%(ext)s")),
                    'format': format_string,
                    'progress_hooks': [progress_hook],
                    'noplaylist': True,
                    'ratelimit': self.settings["speed_limit"] or None,
                    'proxy': self.settings["proxy"] or None,
                    'http_headers': {'User-Agent': self.settings["user_agent"]} if self.settings["user_agent"] else None,
                    'postprocessors': postprocessors if postprocessors else None,
                    'writesubtitles': opts.get("download_subs", False),
                    'subtitleslangs': [opts.get("sub_lang", "en")] if opts.get("download_subs") else None,
                }

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info_dict = ydl.extract_info(url, download=False)
                    filename = ydl.prepare_filename(info_dict)
                    self.update_ui_safe(task_id, filename=os.path.basename(filename))
                    ydl.download([url])
                return (STATUS_COMPLETED, f"Downloaded: {url}")
            except Exception as e:
                return (STATUS_ERROR, f"yt-dlp error: {e}")
        else:
            return (STATUS_ERROR, "yt-dlp is not available. Please install it.")


    def on_download_done(self, future):
        """Callback executed when a download future completes."""
        task_id = None
        for tid, task in self.tasks.items():
            if task["future"] == future:
                task_id = tid
                break
        
        if not task_id:
            return

        try:
            status, message = future.result()
            self.log(message)
            if status == STATUS_COMPLETED:
                self.update_ui_safe(task_id, status=STATUS_COMPLETED)
            else: # Error case
                self.handle_download_error(task_id, message)
        except Exception as e:
            # Exception from the download function itself
            self.handle_download_error(task_id, str(e))
        
        self.process_queue() # Try to start the next download

    def handle_download_error(self, task_id, error_message):
        task = self.tasks[task_id]
        if task["status"] == STATUS_CANCELLED:
            self.log(f"Cancelled: {task['url']}")
            return

        if task["retries"] < self.settings["max_retries"]:
            task["retries"] += 1
            delay = self.settings["retry_delay"]
            self.log(f"Download failed for {task['url']}. Retrying in {delay}s... (Attempt {task['retries']})")
            self.update_ui_safe(task_id, status=f"Retrying ({task['retries']})...")
            self.after(delay * 1000, lambda: self.retry_task(task_id))
        else:
            self.log(f"Download failed permanently for {task['url']}: {error_message}")
            self.update_ui_safe(task_id, status=STATUS_ERROR)

    def retry_task(self, task_id):
        self.tasks[task_id]['status'] = STATUS_QUEUED
        self.download_queue.put(task_id)
        self.process_queue()

    def cancel_task(self, task_id):
        task = self.tasks.get(task_id)
        if not task: return
        
        if task['status'] == STATUS_DOWNLOADING and task['future']:
            # This is a soft cancel. The thread will see the status change and raise an error.
            task['status'] = STATUS_CANCELLED
            self.update_ui_safe(task_id, status=STATUS_CANCELLED)
        elif task['status'] == STATUS_QUEUED:
            task['status'] = STATUS_CANCELLED
            self.update_ui_safe(task_id, status=STATUS_CANCELLED)

    # --- Autopilot and Scheduling ---

    def start_autopilot(self):
        def autopilot_monitor():
            while not self.stop_event.is_set():
                if self.settings["autopilot"]:
                    self.process_queue()
                time.sleep(2) # Check every 2 seconds
        
        threading.Thread(target=autopilot_monitor, daemon=True).start()

    def is_within_schedule(self):
        """Checks if the current time is within the scheduled download window."""
        try:
            now = datetime.datetime.now().time()
            start = datetime.datetime.strptime(self.settings["start_time"], "%H:%M").time()
            end = datetime.datetime.strptime(self.settings["end_time"], "%H:%M").time()

            # Handle overnight schedules (e.g., 22:00 to 06:00)
            if start <= end:
                return start <= now <= end
            else: # Overnight schedule
                return now >= start or now <= end
        except ValueError:
            self.log("Invalid time format in scheduling settings. Ignoring schedule.")
            return True # Default to allow if format is wrong

    # --- Button Commands and Actions ---

    def browse_output(self):
        folder = filedialog.askdirectory()
        if folder:
            self.output_entry.delete(0, tk.END)
            self.output_entry.insert(0, folder)

    def browse_bulk_file(self):
        file = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
        if file:
            self.bulk_file_entry.delete(0, tk.END)
            self.bulk_file_entry.insert(0, file)

    def browse_bulk_output(self):
        folder = filedialog.askdirectory()
        if folder:
            self.bulk_output_entry.delete(0, tk.END)
            self.bulk_output_entry.insert(0, folder)

    def add_single_to_queue(self):
        url = self.url_entry.get().strip()
        output_path = self.output_entry.get().strip()
        if not url:
            messagebox.showerror("Error", "Please enter a URL")
            return
        self.add_task(url, output_path)
        self.url_entry.delete(0, tk.END)
        self.tabview.set("Download Manager")

    def add_bulk_to_queue(self):
        file_path = self.bulk_file_entry.get().strip()
        output_path = self.bulk_output_entry.get().strip()
        if not file_path or not os.path.exists(file_path):
            messagebox.showerror("Error", "Please select a valid text file")
            return
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            for url in urls:
                self.add_task(url, output_path)
            self.log(f"Added {len(urls)} URLs to queue from file.")
            self.tabview.set("Download Manager")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read file: {e}")

    def start_queue(self):
        self.stop_event.clear()
        self.log("Manual queue start initiated.")
        self.process_queue()

    def stop_queue(self):
        self.stop_event.set()
        # Clear the queue to prevent new tasks from starting
        while not self.download_queue.empty():
            try:
                self.download_queue.get_nowait()
            except queue.Empty:
                break
        # Cancel all active and queued tasks
        for task_id, task in self.tasks.items():
            if task['status'] in [STATUS_QUEUED, STATUS_DOWNLOADING]:
                self.cancel_task(task_id)
        self.log("Stop command issued. All active downloads cancelled.")

    def clear_completed(self):
        completed_ids = [tid for tid, task in self.tasks.items() 
                         if task["status"] in [STATUS_COMPLETED, STATUS_ERROR, STATUS_CANCELLED]]
        for tid in completed_ids:
            self.tasks[tid]["ui"]["frame"].destroy()
            del self.tasks[tid]
        self.log("Cleared completed tasks from view.")

    def open_file_location(self, path):
        if not path: return
        folder = os.path.dirname(path)
        try:
            if sys.platform == "win32":
                os.startfile(folder)
            elif sys.platform == "darwin": # macOS
                subprocess.Popen(["open", folder])
            else: # linux
                subprocess.Popen(["xdg-open", folder])
        except Exception as e:
            self.log(f"Error opening folder: {e}")

    # --- Settings Management ---

    ### NEW ### - Method to open the config window
    def open_yt_dlp_config_window(self):
        # Pass the current detailed settings to the new window
        YTDLPConfigWindow(self, self.settings.get("yt_dlp_options", {}))
    
    ### NEW ### - Method for the config window to call back to update settings
    def update_yt_dlp_settings(self, new_options):
        self.settings["yt_dlp_options"] = new_options
        self.log("Advanced yt-dlp options updated.")
        self.update_format_summary() # Update the summary field
    
    ### NEW ### - Helper to generate the summary string for the main settings UI
    def update_format_summary(self):
        opts = self.settings.get("yt_dlp_options", {})
        video = opts.get("video_quality", "best")
        audio = opts.get("audio_quality", "best")
        summary = f"Video: {video}, Audio: {audio}"
        if opts.get("download_subs"):
            summary += f", Subs: {opts.get('sub_lang', 'en')}"
        
        # Update the readonly entry field
        self.format_entry.configure(state="normal")
        self.format_entry.delete(0, tk.END)
        self.format_entry.insert(0, summary)
        self.format_entry.configure(state="readonly")


    def update_sim_downloads_display(self, value):
        self.sim_downloads_label.configure(text=str(int(value)))

    def change_theme(self, new_theme):
        ctk.set_appearance_mode(new_theme.lower())

    def save_settings(self):
        try:
            # General Settings
            new_max_workers = int(self.sim_downloads_slider.get())
            if new_max_workers != self.settings["simultaneous_downloads"]:
                self.executor.shutdown(wait=False, cancel_futures=True)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=new_max_workers)
                self.settings["simultaneous_downloads"] = new_max_workers
                self.log(f"Simultaneous downloads set to {new_max_workers}. Restarting thread pool.")

            self.settings["max_retries"] = int(self.max_retries_entry.get())
            # self.settings["output_format"] is now managed by update_yt_dlp_settings
            self.settings["proxy"] = self.proxy_entry.get()
            self.settings["user_agent"] = self.ua_entry.get()
            self.settings["speed_limit"] = self.speed_entry.get()
            self.settings["autopilot"] = self.autopilot_var.get()
            self.settings["use_yt_dlp"] = self.yt_dlp_var.get()
            self.settings["theme"] = self.theme_menu.get().lower()
            
            # Scheduling Settings
            self.settings["enable_scheduling"] = self.schedule_var.get()
            self.settings["start_time"] = self.start_time_entry.get()
            self.settings["end_time"] = self.end_time_entry.get()
            
            with open(SETTINGS_FILE, "w") as f:
                json.dump(self.settings, f, indent=4)
            self.log("Settings saved successfully.")
            messagebox.showinfo("Settings Saved", "All settings have been saved successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {e}")

    def load_settings(self):
        settings = DEFAULT_SETTINGS.copy()
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    loaded_settings = json.load(f)
                # Deep update for nested dictionaries like yt_dlp_options
                for key, value in loaded_settings.items():
                    if isinstance(value, dict) and key in settings:
                        settings[key].update(value)
                    else:
                        settings[key] = value
            except json.JSONDecodeError:
                logging.error("Could not decode settings.json, using defaults.")
        return settings

    def log(self, message):
        logging.info(message)
        log_entry = f"{time.strftime('%H:%M:%S')} - {message}\n"
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def on_closing(self):
        active_downloads = any(t["status"] == STATUS_DOWNLOADING for t in self.tasks.values())
        if active_downloads:
            if not messagebox.askyesno("Confirm Exit", "Downloads are in progress. Are you sure you want to exit?"):
                return
        
        self.stop_event.set()
        self.executor.shutdown(wait=False, cancel_futures=True)
        # No need to call save_settings() here as it's for the button.
        # The settings are already updated in memory. We can save them silently.
        try:
            with open(SETTINGS_FILE, "w") as f:
                json.dump(self.settings, f, indent=4)
        except Exception as e:
            print(f"Could not save settings on exit: {e}") # print instead of log as logger might be closed
        self.destroy()

if __name__ == "__main__":
    if not YT_DLP_AVAILABLE:
        root = tk.Tk()
        root.withdraw()
        messagebox.showwarning("Dependency Missing",
                               "yt-dlp is not installed. Most download functions will not work.\n\n"
                               "Please install it by running:\n"
                               "pip install yt-dlp")
        root.destroy()
    
    app = M3UDLApp()
    app.mainloop()