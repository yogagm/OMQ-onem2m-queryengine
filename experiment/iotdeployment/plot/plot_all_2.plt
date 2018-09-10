set key inside right
set key box linestyle 1 maxrows 4
set key box width 1
set style line 1 lt 1 lw 1 pt 2
set style line 2 lt 1 lw 1 pt 6
set style line 3 lt 1 lw 1 pt 4
set style line 4 lt 1 lw 1 pt 8
set style line 5 lt 1 lw 1 pt 10
set style line 6 lt 1 lw 1 pt 12
set style data linespoints
set pointsize 2
set xtics rotate



set ylabel "TX Rate of IoT Nodes (kB/s)"
set term postscript eps enhanced "Helvetica" 20
set yrange[0:60]
set output 'bw_log_cse.eps'
plot 'bw_log_cse.txt' using 2:xticlabels(1) title column ls 3,\
     'bw_log_cse.txt' using 3:xticlabels(1) title column ls 4,\
     'bw_log_cse.txt' using 4:xticlabels(1) title column ls 5,\
     'bw_log_cse.txt' using 5:xticlabels(1) title column ls 6
     
set ylabel "TX Rate of IoT Nodes (kB/s)"
set yrange[0:2]
set term postscript eps enhanced "Helvetica" 20
set output 'bw_log_qe.eps'
plot  'bw_log_qe.txt' using 2:xticlabels(1) title column ls 3,\
     'bw_log_qe.txt' using 3:xticlabels(1) title column ls 4,\
     'bw_log_qe.txt' using 4:xticlabels(1) title column ls 5,\
     'bw_log_qe.txt' using 5:xticlabels(1) title column ls 6
     
set ylabel "TX Rate of the Infrastructure Node (kB/s)"
set term postscript eps enhanced "Helvetica" 20
set yrange[0:20]
set output 'bw_log_in.eps'
plot 'bw_log_cse.txt' using 6:xticlabels(1) title "without-qe" ls 1,\
     'bw_log_qe.txt' using 6:xticlabels(1) title "with-qe" ls 2
     
     
set ylabel "Average RAM Usage of IoT Nodes (MB)"
set yrange[300:460]
set term postscript eps enhanced "Helvetica" 20
set output 'mem_log.eps'
plot 'mem_log_cse.txt' using 7:xticlabels(1) title "without-qe" ls 1,\
     'mem_log_qe.txt' using 7:xticlabels(1) title "with-qe" ls 2
     
set ylabel "Average CPU usage of IoT Nodes (%)"
set yrange[*:10]
set term postscript eps enhanced "Helvetica" 20
set output 'cpu_log.eps'
plot 'cpu_log_cse.txt' using 7:xticlabels(1) title "without-qe" ls 1,\
     'cpu_log_qe.txt' using 7:xticlabels(1) title "with-qe" ls 2

set style data histogram
set style histogram cluster gap 1
set style fill pattern 2
set boxwidth 0.8
set xtic scale 0
set ylabel "Processing time (ms)"
set yrange[0:150]
set term postscript eps enhanced "Helvetica" 20
set output 'delay.eps'
plot 'delay_log_cse.txt' using 2:xticlabels(1) title column,\
     'delay_log_qe.txt' using 2:xticlabels(1) title column 
