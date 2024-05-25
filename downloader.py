import os
import json
from multiprocessing import Pool

def download(videoid,path):
    """
    videoid:从json读取的“videoID”列的值
    path:下载路径
    """
    # 跳过已下载的
    if os.path.exists("%s\\%s.mp4" % (path,videoid)):
        print("[DownLoaded] %s has been downloaded." % (videoid))
        return
    # 把起止时间从json中的八位秒数，转换为正常秒数，之后再转换为 xx：xx：xx的形式
    section = [int(videoid[12:18]), int(videoid[19:])]
    print("[TimeSection]: ",section)
    start_time = end_time = ""
    for i in range(2, -1, -1):
        start_time += "%s:" % str(section[0] // (60 ** i)).zfill(2)
        section[0] -= (60 ** i) * (section[0] // (60 ** i))
        end_time += "%s:" % str(section[1] // (60 ** i)).zfill(2)
        section[1] -= (60 ** i) * (section[1] // (60 ** i))
    start_time = start_time[:8]
    end_time = end_time[:8]
    print("[TimeSection]: %s - %s"% (start_time,end_time))
    # 存储参数的字典
    params = {
        "format": "bv[ext=mp4]",
        "name1": "cache\\%s.mp4" % (videoid),
        "name2": "%s\\%s.mp4" % (path,videoid),
        "start": start_time,
        "end": end_time,
        "proxy": "HTTP://127.0.0.1:7890",
        "url": "https://www.youtube.com/watch?v=%s" % (videoid[:11]),
    }
    # yt-dlp下载
    os.system(
        "yt-dlp.exe -i -f %s -o %s -R 2 --proxy %s %s"
        % (params["format"], params["name1"], params["proxy"], params["url"],))
    # 视频切割
    if os.path.exists("%s" % (params["name1"])):
        os.system(
            "ffmpeg\\bin\\ffmpeg.exe -hwaccel auto -i  %s  -ss %s -to %s -async 1 %s -y"
            % (params["name1"], params["start"], params["end"], params["name2"])
        )
    # 删除原视频
    try:
        os.remove("%s" % (params["name1"]))
        print("[DownLoaded] %s has been downloaded." % (videoid))
    except:
        pass


def multi_thread(dataset, start, end,path):
    for i in range(start, end):
        print("[DownLoading]: %s.mp4\t[INDEX]: %d" % (dataset[i]['videoID'],i))
        download(dataset[i]['videoID'],path)


def cut(length, num):
    spilt = [0, ]
    node = (length // num) + 1
    for i in range(num):
        if length % num == 0:
            spilt.append((node - 1) * (i + 1))
        else:
            if i < (num - 1):
                spilt.append(node * (i + 1))
            else:
                spilt.append(length % node + node * i)
    return spilt


if __name__ == '__main__':

    params={
    "json":["vatex_validation_v1.0.json","vatex_training_v1.0.json","vatex_private_test_without_annotations.json","vatex_public_test_english_v1.1.json"],
    "path":["val","train","private_test","public_test"],
    "thread_num":16,
    }

    for i in range(len(params["json"])):

        # 打开文件，读取json
        with (open(params["json"][i], 'r') as json_file):
            dataset_json = json.load(json_file)

        # 根据线程数生成分割
        thread_num = params["thread_num"]
        length = len(dataset_json)
        spilt = cut(length, thread_num)
        print(spilt)
        #创建多进程池
        pol = Pool(thread_num)
        for j in range(thread_num):
            pol.apply_async(multi_thread,(dataset_json,spilt[j],spilt[j+1],params["path"][i]))
        pol.close()
        pol.join()
        del pol
        del dataset_json