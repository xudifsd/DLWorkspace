cwd=`dirname $0`

mkdir -p /etc/ssh
cp $cwd/etc/* /etc/ssh
cp $cwd/init.d/* /etc/init.d
chmod +x /etc/init.d/ssh
cp $cwd/default/* /etc/default
cp -r $cwd/bin $cwd/sbin $cwd/lib $cwd/libexec /usr/
