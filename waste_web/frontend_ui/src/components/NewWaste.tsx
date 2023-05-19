import React, { ChangeEventHandler, MouseEventHandler, useEffect, useRef, useState } from 'react';
import { X, FileUp, Loader } from 'lucide-react';

import { Button } from './Button';
import useCreateWastes from '../hooks/useCreateWaste';

interface WasteItemProps {
  name: string;
  type: string;
  onCancelClick: React.MouseEventHandler<HTMLButtonElement>;
}

function WasteItem(props: WasteItemProps) {
  return (
    <div className="flex p-2 rounded bg-gray-100">
      <div className="flex-1 overflow-hidden">
        <div className="">
          {props.name}
        </div>
        <div className='text-gray-500'>{props.type}</div>
      </div>
      <div>
        <Button onClick={props.onCancelClick} variant='no-outline' size='reset'>
          <X />
        </Button>
      </div>
    </div>
  );
}

export function NewWaste() {
  const [files, setFiles] = useState(new Set() as Set<File>);
  const fileInputRef = useRef(null as HTMLInputElement | null);
  const createWastes = useCreateWastes();
  const [showCreateWasteBox, setShowCreateWasteBox] = useState(false);
  const [submited, setSubmited] = useState(false);

  const handleFileChange: ChangeEventHandler<HTMLInputElement> = (e) => {
    console.log('Clicked', files);
    const newFiles = new Set([
      ...files,
      ...(e.target.files ?? []),
    ]);
    console.debug(newFiles);
    setFiles(newFiles);
  }

  const handleSubmit: MouseEventHandler<HTMLButtonElement> = (_e) => {
    setSubmited(true);
    createWastes.mutate(Array.from(files, (f) => ({
      metadata: {
        filename: f.name,
        contentType: f.type,
      },
      data: f,
    })))
  }

  useEffect(() => {
    if (submited && !createWastes.isLoading) {
      setSubmited(false);
      setFiles(new Set());
      setShowCreateWasteBox(false);
    }
  }, [createWastes.isLoading]);

  return (
    <>
      <Button onClick={() => setShowCreateWasteBox(true)}>
        New Waste
      </Button>
      {showCreateWasteBox &&
        <div className="flex justify-center items-center fixed inset-0 z-50 bg-gray-950 bg-opacity-50">
          <div className="flex flex-col gap-3 px-3 py-4 bg-white w-[30rem] rounded">
            <div className="flex items-center">
              <span>Add Waste</span>
              <span className="flex-1"/>
              <Button onClick={() => setShowCreateWasteBox(false)} variant='no-outline' size='reset'>
                <X size={20} absoluteStrokeWidth className="text-gray-500" />
              </Button>
            </div>
            <div
              className="flex flex-col gap-1 justify-center items-center h-[8rem] w-full border border-2 border-gray-300 border-dashed rounded text-gray-800"
              onDrag={() => {}}
            >
              <FileUp />
              <span>
                Drag & Drop or{" "}
                <span
                  className='text-cyan-600 cursor-pointer'
                  onClick={() => {
                    fileInputRef.current?.click()
                  }}
                >
                  Choose file
                </span>
                {" "}to upload
              </span>
              <input
                ref={fileInputRef}
                type='file'
                multiple
                className='hidden'
                onClick={e => {
                  (e.target as any).value = "";
                }}
                onChange={handleFileChange}
              />
            </div>
            <div className="flex flex-col gap-3 max-h-60 overflow-auto">
              {Array.from(files, (file, n) => (
                <WasteItem
                  key={n}
                  name={file.name}
                  type={file.type}
                  onCancelClick={() => {
                    files.delete(file);
                    setFiles(new Set(files));
                  }}
                />
              ))}
            </div>
            <div className="flex gap-4">
              <span className="flex-1"/>
              <Button variant='cancel' onClick={() => setShowCreateWasteBox(false)}>Cancel</Button>
              <Button onClick={handleSubmit}>
                {createWastes.isLoading
                  ? <Loader />
                  : <>Submit</>}
              </Button>
            </div>
          </div>
        </div>
      }
    </>
  )
};