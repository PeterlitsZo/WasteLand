import React from "react";

interface ButtonProps {
  children: React.ReactNode;
  variant?: 'cancel' | 'no-outline';
  size?: 'reset';
  onClick?: React.MouseEventHandler<HTMLButtonElement>;
}

export function Button(props: ButtonProps) {
  const colorClassName = {
    'default': 'bg-green-950 text-white',
    'cancel': 'border border-gray-300 text-gray-500',
    'no-outline': '',
  }[props.variant ?? 'default'];

  const sizeClassName = {
    'default': 'px-2.5 py-1 h-8',
    'reset': '',
  }[props.size ?? 'default'];

  return (
    <button className={`${sizeClassName} rounded ${colorClassName}`} onClick={props.onClick}>
      { props.children }
    </button>
  )
}