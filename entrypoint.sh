if [ $STARTUP_APP == "sleep" ]; then 
    sleep infinity
else
    /ape-dts $1
fi