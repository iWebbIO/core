import pynput
from pynput.keyboard import Key, Listener
from pynput.mouse import Button, Controller
import pyperclip

# Initialize mouse controller
mouse = Controller()

alt_pressed = False

def on_press(key):
    global alt_pressed
    try:
        if key == Key.ctrl_r:  # Right control key
            print("Right Control pressed - simulating middle click")
            mouse.click(Button.middle)  # Simulate middle mouse click
        elif key == Key.alt_gr:  # Right alt key (alt gr)
            if not alt_pressed:
                alt_pressed = True
                clipboard_content = pyperclip.paste()
                print(f"Right Alt pressed - appending clipboard content to clip.txt: {clipboard_content}")
                with open("clip.txt", "a", encoding="utf-8") as f:
                    f.write(clipboard_content + "\n")
        else:
            print(f"Key pressed: {key}")  # Debug: print unrecognized keys
    except AttributeError:
        print(f"Special key pressed: {key}")  # Handle special keys

def on_release(key):
    global alt_pressed
    if key == Key.alt_gr:  # Reset alt pressed flag when right alt is released
        alt_pressed = False
    elif key == Key.esc:  # Stop the script when Escape is pressed
        print("Escape pressed - stopping script")
        return False

# Set up the listener
with Listener(on_press=on_press, on_release=on_release) as listener:
    print("Script is running. Press Right Control to simulate middle click, Right Alt to append clipboard to clip.txt. Press Escape to stop.")
    listener.join()  # Keep running until stopped
