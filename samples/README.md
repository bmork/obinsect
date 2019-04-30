
One way to turn these ascii dumps back into binary HDLC frames:

perl -e 'while (<>) { chomp; s/\s*//g; $x .= $_ }; print pack("C*", map {hex} unpack("(A2)*", $x))' < aidon-han-if-descr-list2.txt 


