import sounddevice as sd
import wave
import numpy as np
import librosa
import threading
import queue

class HumanEar:
    def __init__(self, ear_id):
        self.ear_id = ear_id
        self.chunk_size = 1024
        self.sampling_rate = 44100
        self.pitch = None
        self.volume = None
        self.audio_queue = queue.Queue()

    def process_chunk(self):
        """Process chunks of audio from the queue."""
        while True:
            data = self.audio_queue.get()
            if data is None:  # Signal to stop processing
                break
            self.pitch = self._get_pitch(data, self.sampling_rate)
            self.volume = self._get_volume(data)
            # Additional processing for localization or other effects can be added here

    def _get_pitch(self, audio_data, sampling_rate):
        """Extract the pitch from the audio data."""
        pitches, magnitudes = librosa.piptrack(y=audio_data, sr=sampling_rate)
        pitch = np.max(pitches)
        return pitch

    def _get_volume(self, audio_data):
        """Calculate the volume (RMS amplitude) from the audio data."""
        rms = librosa.feature.rms(y=audio_data)[0]
        volume = np.mean(rms)
        return volume

    def add_chunk(self, data):
        """Add a chunk of data to the queue."""
        self.audio_queue.put(data)

    def stop_processing(self):
        """Stop processing and clear the queue."""
        self.audio_queue.put(None)


class Ears:
    def __init__(self):
        self.left_ear = HumanEar('left')
        self.right_ear = HumanEar('right')
        self.left_thread = threading.Thread(target=self.left_ear.process_chunk)
        self.right_thread = threading.Thread(target=self.right_ear.process_chunk)

    def start_processing(self):
        self.left_thread.start()
        self.right_thread.start()

    def stop_processing(self):
        self.left_ear.stop_processing()
        self.right_ear.stop_processing()
        self.left_thread.join()
        self.right_thread.join()

    def add_chunk(self, left_data, right_data):
        """Add chunks to each ear."""
        self.left_ear.add_chunk(left_data)
        self.right_ear.add_chunk(right_data)


class AudioStreamer:
    def __init__(self, file_path, ears, chunk_size=1024):
        self.file_path = file_path
        self.ears = ears
        self.chunk_size = chunk_size

    def start_streaming(self):
        """Begin streaming audio data to the ears."""
        with wave.open(self.file_path, 'rb') as wav_file:
            assert wav_file.getnchannels() == 2, "The WAV file must be stereo."

            while True:
                frames = wav_file.readframes(self.chunk_size)
                if not frames:
                    break

                # Convert the byte data to numpy array
                data = np.frombuffer(frames, dtype=np.int16)
                # Split the stereo audio into two channels
                left_data = data[0::2]
                right_data = data[1::2]

                self.ears.add_chunk(left_data, right_data)


def test():
    # Usage
    ears = Ears()
    ears.start_processing()

    # Create an AudioStreamer instance
    streamer = AudioStreamer('path/to/your/audiofile.wav', ears)
    streamer.start_streaming()

    ears.stop_processing()

if __name__ == '__main__':
    test()
