import os

class FolderLister:
  def __init__(self, folder_path):
    self.folder_path = folder_path

  def list_folders(self):
    folder_list = []
    for item in os.listdir(self.folder_path):
      item_path = os.path.join(self.folder_path, item)
      if os.path.isdir(item_path):
        folder_list.append(item)
    return folder_list
  
  def list_files(self):
        file_list = []
        for folder in self.list_folders():
            folder_path = os.path.join(self.folder_path, folder)
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                if os.path.isfile(item_path):
                    file_list.append(item)
        return file_list
  
  def list_rel_paths(self):
        file_list = []
        for folder in self.list_folders():
            folder_path = os.path.join(self.folder_path, folder)
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                if os.path.isfile(item_path):
                    relative_path = os.path.relpath(item_path, self.folder_path)
                    relative_path = self.folder_path + '/' + relative_path
                    file_list.append(relative_path)
        return file_list
