import useGetWaste from "../hooks/useGetWaste";
import useListWastes from "../hooks/useListWastes";
import { WasteId } from "../utils/WasteId";
import useBaseUrl from "../hooks/useBaseUrl";
import { File } from "lucide-react";

export function WasteList() {
  const wasteList = useListWastes();

  return (
    <div className="overflow-auto p-3">
      {wasteList.isLoading
        ? <>Loading...</>
        : <WasteListInner wasteList={wasteList.data!} />}
    </div>
  );
}

interface WasteListInnerProps {
  wasteList: WasteId[];
}

function WasteListInner(props: WasteListInnerProps) {
  return (
    <div className="grid gap-3 grid-cols-[repeat(auto-fill,_minmax(176px,_1fr))]">
      {props.wasteList.map(wasteId => <WasteItem key={wasteId} id={wasteId} />)}
    </div>
  );
}

interface WasteItemProps {
  id: WasteId;
}

function WasteItem(props: WasteItemProps) {
  const { isLoading, error: _error, data } = useGetWaste(props.id);

  const url = useBaseUrl() + 'api/v1/wastes/' + props.id;

  const contentPreview = isLoading ? 'Loading...' : (() => {
    if (data!.metadata.contentType.startsWith('image/')) {
      return (
        <img
          src={url}
          className="w-full h-full object-cover"
          style={{ 'background': 'repeating-conic-gradient(#dddddd 0% 25%, transparent 0% 50%) 50% / 20px 20px' }}
        />
      );
    } else {
      return (
        <div className="w-full h-full grid place-content-center">
          <File />
        </div>
      );
    }
  })();

  return (
    <div className="flex flex-col h-48 border border-gray-100 hover:border-gray-500 cursor-pointer bg-gray-100 rounded overflow-hidden">
      <div className="flex-1 overflow-hidden bg-white">
        {contentPreview}
      </div>
      <div className="p-2">
        <div>
          {props.id}
        </div>
        <div className="text-gray-500">
          {isLoading
            ? 'Loading...'
            : data!.metadata.contentType}
        </div>
      </div>
    </div>
  )
}