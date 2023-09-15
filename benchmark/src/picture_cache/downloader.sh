IMAGE_LIST_FILE=./image_list.txt
DOWNLOAD_FOLDER=~/tmp/waste_land_picture_cache
python3 downloader.py $IMAGE_LIST_FILE --download_folder=$DOWNLOAD_FOLDER --num_processes=24