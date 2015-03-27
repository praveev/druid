echo $(fgrep '<version>' pom.xml | sed -n 2p | grep -oP '\d+\.\d+\.\d+')-$(date '+%s')-$(git rev-parse --short HEAD)-${BUILD_NUMBER}
