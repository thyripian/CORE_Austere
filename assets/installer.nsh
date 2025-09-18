; Custom NSIS installer script for CORE Scout
!macro customInstall
  ; Create desktop shortcut
  CreateShortCut "$DESKTOP\CORE Scout.lnk" "$INSTDIR\Scout.exe" "" "$INSTDIR\Scout.exe" 0
  
  ; Create Start Menu folder
  CreateDirectory "$SMPROGRAMS\CORE Scout"
  CreateShortCut "$SMPROGRAMS\CORE Scout\CORE Scout.lnk" "$INSTDIR\Scout.exe" "" "$INSTDIR\Scout.exe" 0
  CreateShortCut "$SMPROGRAMS\CORE Scout\Uninstall.lnk" "$INSTDIR\Uninstall.exe" "" "$INSTDIR\Uninstall.exe" 0
!macroend

!macro customUnInstall
  ; Remove desktop shortcut
  Delete "$DESKTOP\CORE Scout.lnk"
  
  ; Remove Start Menu folder
  RMDir /r "$SMPROGRAMS\CORE Scout"
!macroend

; Custom installer page
!macro customHeader
  !system "echo 'CORE Scout - Standalone CORE Offline Utility Tool' > ${BUILD_RESOURCES_DIR}\installer-header.txt"
!macroend
