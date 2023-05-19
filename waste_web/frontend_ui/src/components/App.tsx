import { ChevronLeft, ChevronRight } from 'lucide-react';

import useListWastes from "../hooks/useListWastes";
import { NewWaste } from './NewWaste';
import { WasteList } from './WasteList';

function App() {
  let listWastes = useListWastes();

  console.log(listWastes);

  return (
    <div className="flex flex-col h-screen">
      <WasteList />
      <div className="flex-1"></div>
      <div className="flex items-center h-12 px-3 bg-gray-200 shadow-upper-xl border-t border-gray-300">
        <NewWaste />
        <span className="flex-1"/>
        <div className="flex gap-3 items-center">
          <button className="flex items-center place-content-center rounded-full bg-green-950 text-white h-5 w-5">
            <ChevronLeft size={12} absoluteStrokeWidth />
          </button>
          {1} / {14} Page
          <button className="flex items-center place-content-center rounded-full bg-green-950 text-white h-5 w-5">
            <ChevronRight size={12} absoluteStrokeWidth />
          </button>
        </div>
      </div>
    </div>
  )
}

export default App
