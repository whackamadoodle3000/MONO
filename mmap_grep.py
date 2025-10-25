#!/usr/bin/env python3
"""
Fast grep using memory mapping.
Loads files into memory once, then searches super fast.
"""

import os
import mmap
import time
import subprocess
from pathlib import Path
from queue import Queue
from dataclasses import dataclass
from typing import List, Dict, Optional
import threading
import re
from collections import Counter


@dataclass
class Codebase:
    name: str
    path: str
    files: Dict[str, mmap.mmap]  # file -> mmap
    stats: Dict[str, dict]  # file -> info


@dataclass
class SearchRequest:
    codebase: str
    pattern: str
    case_sensitive: bool = True
    max_results: Optional[int] = None


@dataclass
class SearchResult:
    file: str
    line: int
    text: str
    pos: int


@dataclass
class FileInfo:
    file: str
    size: int
    ext: str
    mtime: float
    lines: int
    words: int


@dataclass
class Stats:
    files: int
    size: int
    lines: int
    words: int
    types: Dict[str, int]
    size_by_ext: Dict[str, int]
    lines_by_ext: Dict[str, int]
    biggest: List[FileInfo]
    common_words: List[tuple]


class FastGrep:
    """Super fast grep using memory mapping."""
    
    def __init__(self):
        self.codebases = {}
        self.queue = Queue()
        self.running = False
        self.worker = None
        self.word_re = re.compile(r'\b\w+\b')
        
    def load(self, name: str, path: str, exts: Optional[List[str]] = None) -> bool:
        """Load a codebase into memory."""
        if not os.path.isdir(path):
            print(f"Bad path: {path}")
            return False
            
        if exts is None:
            exts = ['.py', '.txt', '.md', '.js', '.java', '.c', '.cpp', 
                   '.h', '.go', '.rs', '.ts', '.jsx', '.tsx', '.html', 
                   '.sh', '.sample', '.yml', '.yaml', '.json', '.xml']
        
        files = {}
        stats = {}
        root = Path(path)
        
        print(f"Loading {name} from {path}...")
        
        # Get all files, skip binary stuff
        all_files = []
        for f in root.rglob('*'):
            if f.is_file():
                if f.suffix.lower() in ['.png', '.jpg', '.jpeg', '.gif', '.ico', '.pdf', '.zip', '.tar', '.gz', '.exe', '.dll', '.so', '.dylib']:
                    continue
                all_files.append(f)
        
        # Filter by extensions if specified
        if exts:
            all_files = [f for f in all_files if f.suffix in exts]
        
        # Load each file into memory
        for f in all_files:
            try:
                stat_info = f.stat()
                fpath = str(f)
                
                with open(f, 'r+b') as file:
                    if file.seek(0, 2) > 0:  # Check if not empty
                        file.seek(0)
                        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
                        try:
                            mm.madvise(mmap.MADV_WILLNEED)
                        except:
                            pass  # Some systems don't support this
                        
                        files[fpath] = mm
                        
                        # Count stuff
                        content = mm.read()
                        lines = content.split(b'\n')
                        words = self.word_re.findall(content.decode('utf-8', errors='replace'))
                        
                        stats[fpath] = {
                            'size': stat_info.st_size,
                            'ext': f.suffix,
                            'mtime': stat_info.st_mtime,
                            'lines': len(lines),
                            'words': len(words)
                        }
                            
            except Exception as e:
                print(f"Couldn't load {f}: {e}")
        
        if not files:
            print(f"No files found in {path}")
            return False
            
        self.codebases[name] = Codebase(
            name=name,
            path=path,
            files=files,
            stats=stats
        )
        
        print(f"Loaded {len(files)} files for {name}")
        return True
    
    def unload(self, name: str):
        """Unload a codebase."""
        if name in self.codebases:
            cb = self.codebases[name]
            for mm in cb.files.values():
                mm.close()
            del self.codebases[name]
            print(f"Unloaded {name}")
    
    
    def stats(self, name: str) -> Stats:
        """Get stats for a codebase."""
        if name not in self.codebases:
            print(f"No codebase: {name}")
            return None
        
        cb = self.codebases[name]
        
        # Count everything
        total_files = len(cb.files)
        total_size = sum(s['size'] for s in cb.stats.values())
        total_lines = sum(s['lines'] for s in cb.stats.values())
        total_words = sum(s['words'] for s in cb.stats.values())
        
        # File types
        types = Counter(s['ext'] for s in cb.stats.values())
        
        # By extension
        size_by_ext = {}
        lines_by_ext = {}
        for s in cb.stats.values():
            ext = s['ext']
            size_by_ext[ext] = size_by_ext.get(ext, 0) + s['size']
            lines_by_ext[ext] = lines_by_ext.get(ext, 0) + s['lines']
        
        # Biggest files
        file_infos = []
        for fpath, s in cb.stats.items():
            file_infos.append(FileInfo(
                file=fpath,
                size=s['size'],
                ext=s['ext'],
                mtime=s['mtime'],
                lines=s['lines'],
                words=s['words']
            ))
        
        biggest = sorted(file_infos, key=lambda x: x.size, reverse=True)[:10]
        
        # Common words
        word_count = Counter()
        for fpath, mm in cb.files.items():
            mm.seek(0)
            content = mm.read().decode('utf-8', errors='replace')
            words = self.word_re.findall(content.lower())
            word_count.update(w for w in words if len(w) > 2)
        
        common_words = word_count.most_common(20)
        
        return Stats(
            files=total_files,
            size=total_size,
            lines=total_lines,
            words=total_words,
            types=dict(types),
            size_by_ext=size_by_ext,
            lines_by_ext=lines_by_ext,
            biggest=biggest,
            common_words=common_words
        )
    
    
    def file_info(self, name: str, filepath: str) -> Optional[FileInfo]:
        """Get info about a specific file."""
        if name not in self.codebases:
            print(f"No codebase: {name}")
            return None
        
        cb = self.codebases[name]
        
        if filepath not in cb.stats:
            print(f"File not found: {filepath}")
            return None
        
        s = cb.stats[filepath]
        return FileInfo(
            file=filepath,
            size=s['size'],
            ext=s['ext'],
            mtime=s['mtime'],
            lines=s['lines'],
            words=s['words']
        )
    
    def search(self, request: SearchRequest) -> List[SearchResult]:
        """Search for a pattern."""
        if request.codebase not in self.codebases:
            print(f"No codebase: {request.codebase}")
            return []
        
        cb = self.codebases[request.codebase]
        results = []
        
        # Compile regex
        flags = 0 if request.case_sensitive else re.IGNORECASE
        try:
            pattern = re.compile(request.pattern.encode('utf-8'), flags)
        except re.error as e:
            print(f"Bad regex: {e}")
            return []
        
        # Search each file
        for fpath, mm in cb.files.items():
            mm.seek(0)
            content = mm.read()
            
            lines = content.split(b'\n')
            for i, line in enumerate(lines, 1):
                match = pattern.search(line)
                if match:
                    try:
                        line_str = line.decode('utf-8', errors='replace')
                        results.append(SearchResult(
                            file=fpath,
                            line=i,
                            text=line_str.strip(),
                            pos=match.start()
                        ))
                        
                        if request.max_results and len(results) >= request.max_results:
                            return results
                    except Exception as e:
                        print(f"Error in {fpath}: {e}")
        
        return results
    
    def add_search(self, request: SearchRequest):
        """Add search to queue."""
        self.queue.put(request)
    
    def start_worker(self):
        """Start background worker."""
        if self.running:
            print("Worker already running")
            return
        
        self.running = True
        self.worker = threading.Thread(target=self._worker, daemon=True)
        self.worker.start()
        print("Worker started")
    
    def stop_worker(self):
        """Stop background worker."""
        self.running = False
        if self.worker:
            self.worker.join(timeout=5)
        print("Worker stopped")
    
    def _worker(self):
        """Background worker loop."""
        while self.running:
            try:
                request = self.queue.get(timeout=1)
                results = self.search(request)
                print(f"\nSearch done: '{request.pattern}' in '{request.codebase}'")
                print(f"Found {len(results)} matches")
                self.queue.task_done()
            except:
                continue  # Timeout or empty queue


def shell_grep(path: str, pattern: str, case_sensitive: bool = True) -> List[SearchResult]:
    """Run grep via shell."""
    cmd = ['grep', '-rn']
    if not case_sensitive:
        cmd.append('-i')
    cmd.extend([pattern, path])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        results = []
        
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split(':', 2)
            if len(parts) >= 3:
                results.append(SearchResult(
                    file=parts[0],
                    line=int(parts[1]),
                    text=parts[2].strip(),
                    pos=0
                ))
        
        return results
    except subprocess.TimeoutExpired:
        print("Grep timed out")
        return []
    except Exception as e:
        print(f"Grep error: {e}")
        return []


def benchmark(path: str, patterns: List[str], runs: int = 3):
    """Compare mmap vs shell grep."""
    print("=" * 70)
    print("BENCHMARK: Mmap vs Shell Grep")
    print("=" * 70)
    print(f"Path: {path}")
    print(f"Patterns: {patterns}")
    print(f"Runs: {runs}")
    print()
    
    # Setup
    grep = FastGrep()
    print("Setting up...")
    setup_start = time.time()
    grep.load('bench', path)
    setup_time = time.time() - setup_start
    print(f"Setup: {setup_time:.3f}s")
    print()
    
    # Verify results match
    print("-" * 70)
    print("VERIFICATION")
    print("-" * 70)
    
    all_good = True
    for pattern in patterns:
        print(f"\nChecking: '{pattern}'")
        
        shell_results = shell_grep(path, pattern)
        shell_count = len(shell_results)
        
        request = SearchRequest(codebase='bench', pattern=pattern)
        mmap_results = grep.search(request)
        mmap_count = len(mmap_results)
        
        print(f"  Shell:  {shell_count} matches")
        print(f"  Mmap:   {mmap_count} matches")
        
        if shell_count != mmap_count:
            print(f"  ⚠️  MISMATCH!")
            all_good = False
        else:
            print(f"  ✅ Match!")
    
    print(f"\nOverall: {'✅ PASSED' if all_good else '⚠️  ISSUES'}")
    print()
    
    # Benchmark shell grep
    print("-" * 70)
    print("SHELL GREP")
    print("-" * 70)
    shell_times = []
    
    for run in range(runs):
        start = time.time()
        for pattern in patterns:
            shell_grep(path, pattern)
        elapsed = time.time() - start
        shell_times.append(elapsed)
        print(f"Run {run + 1}: {elapsed:.3f}s")
    
    shell_avg = sum(shell_times) / len(shell_times)
    print(f"Average: {shell_avg:.3f}s")
    print()
    
    # Benchmark mmap
    print("-" * 70)
    print("MMAP GREP")
    print("-" * 70)
    mmap_times = []
    
    for run in range(runs):
        start = time.time()
        for pattern in patterns:
            request = SearchRequest(codebase='bench', pattern=pattern)
            grep.search(request)
        elapsed = time.time() - start
        mmap_times.append(elapsed)
        print(f"Run {run + 1}: {elapsed:.3f}s")
    
    mmap_avg = sum(mmap_times) / len(mmap_times)
    print(f"Average: {mmap_avg:.3f}s")
    print()
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Shell grep:     {shell_avg:.3f}s")
    print(f"Mmap grep:      {mmap_avg:.3f}s")
    print(f"Setup:          {setup_time:.3f}s")
    
    if mmap_avg < shell_avg:
        speedup = shell_avg / mmap_avg
        print(f"Speedup:        {speedup:.2f}x faster")
        break_even = setup_time / (shell_avg - mmap_avg)
        print(f"Break-even:     ~{break_even:.0f} searches")
    else:
        slowdown = mmap_avg / shell_avg
        print(f"Speedup:        {slowdown:.2f}x slower")
    
    print(f"Accuracy:       {'✅ VERIFIED' if all_good else '⚠️  CHECK'}")
    print("=" * 70)
    
    # Cleanup
    grep.unload('bench')


def demo():
    """Demo the fast grep."""
    print("\n" + "=" * 70)
    print("DEMO: Fast Grep")
    print("=" * 70 + "\n")
    
    # Create test files
    test_dir = Path('/tmp/fast_grep_demo')
    test_dir.mkdir(exist_ok=True)
    
    (test_dir / 'file1.py').write_text('''
def hello_world():
    print("Hello, World!")
    return True

def goodbye_world():
    print("Goodbye, World!")
    return False
''')
    
    (test_dir / 'file2.py').write_text('''
class MyClass:
    def __init__(self):
        self.value = "Hello"
    
    def get_value(self):
        return self.value
''')
    
    (test_dir / 'readme.txt').write_text('''
This is a README file.
It contains some hello text.
And some other content.
''')
    
    (test_dir / 'config.json').write_text('''
{
    "name": "test_project",
    "version": "1.0.0",
    "description": "A test project"
}
''')
    
    print(f"Created test dir: {test_dir}\n")
    
    # Load codebase
    grep = FastGrep()
    grep.load('demo', str(test_dir))
    print()
    
    # Show stats
    print("Stats:")
    print("-" * 70)
    stats = grep.stats('demo')
    if stats:
        print(f"Files: {stats.files}")
        print(f"Size: {stats.size} bytes")
        print(f"Lines: {stats.lines}")
        print(f"Words: {stats.words}")
        print(f"Types: {stats.types}")
        print(f"Size by ext: {stats.size_by_ext}")
        print(f"Lines by ext: {stats.lines_by_ext}")
        print("\nBiggest files:")
        for f in stats.biggest:
            print(f"  {f.file}: {f.size} bytes, {f.lines} lines")
        print("\nCommon words:")
        for word, count in stats.common_words[:10]:
            print(f"  {word}: {count}")
    print()
    
    # Show file info
    print("File info:")
    print("-" * 70)
    py_files = [f for f in grep.codebases['demo'].files.keys() if f.endswith('.py')]
    for fpath in py_files:
        info = grep.file_info('demo', fpath)
        if info:
            print(f"{info.file}:")
            print(f"  Size: {info.size} bytes")
            print(f"  Ext: {info.ext}")
            print(f"  Lines: {info.lines}")
            print(f"  Words: {info.words}")
            print(f"  Modified: {time.ctime(info.mtime)}")
    print()
    
    # Search
    print("Search:")
    print("-" * 70)
    request = SearchRequest(codebase='demo', pattern='hello', case_sensitive=False)
    results = grep.search(request)
    
    for r in results:
        print(f"{r.file}:{r.line}: {r.text}")
    print()
    
    # Background search
    print("Background search:")
    print("-" * 70)
    grep.start_worker()
    
    grep.add_search(SearchRequest('demo', 'World'))
    grep.add_search(SearchRequest('demo', 'class'))
    grep.add_search(SearchRequest('demo', 'return'))
    
    grep.queue.join()
    grep.stop_worker()
    print()
    
    # Cleanup
    grep.unload('demo')
    print("Done!")


if __name__ == '__main__':
    demo()
    
    import os
    nanochat_path = os.path.expanduser('~/nanochat')
    benchmark(nanochat_path, ['def ', 'import ', 'class ', 'return ', 'self'], runs=3)